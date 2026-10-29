"""Diagnostics and route audit compiler for jobs and network routes.

This module provides diagnostic utilities to probe, audit, and compile
telemetry for proxy routes and network interfaces associated with jobs.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DIRECT_ROUTE_KEY = "direct"


def _mask_route_url(raw_url: str | None) -> str:
    """Mask credentials in a route or proxy URL for safe reporting."""
    if not raw_url or raw_url == DIRECT_ROUTE_KEY:
        return DIRECT_ROUTE_KEY
    try:
        parsed = urlparse(raw_url)
        if not parsed.scheme or not parsed.netloc:
            return raw_url
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme}://{host}{port}"
    except Exception:
        return raw_url


class RouteAuditCompiler:
    """Compiles route audits, evaluating interface health, latencies, and anomalies."""

    def __init__(
        self,
        default_timeout_seconds: float = 3.0,
    ) -> None:
        self.default_timeout_seconds = default_timeout_seconds

    def compile(
        self,
        job: Any,
        *,
        proxies: Sequence[Any] | None = None,
        tasks: Sequence[Any] | None = None,
        probe_interfaces: bool = False,
    ) -> dict[str, Any]:
        """Compile a route audit dictionary for the specified job.

        Whenever `job.metadata.get("include_direct_connection")` is False (or falsy),
        the "direct" interface is strictly excluded from the returned route_audit dictionary
        and will not be probed or reported.
        """
        metadata: Mapping[str, Any] = {}
        if hasattr(job, "metadata") and isinstance(job.metadata, dict):
            metadata = job.metadata
        elif isinstance(job, dict):
            metadata = job.get("metadata") or {}

        include_direct = bool(metadata.get("include_direct_connection", False))

        route_audit: dict[str, Any] = {}

        # 1. Collect declared proxy routes
        declared_routes: list[dict[str, Any]] = []
        if proxies:
            for p in proxies:
                if isinstance(p, str):
                    declared_routes.append({"url": p, "display_url": _mask_route_url(p)})
                elif isinstance(p, dict):
                    url = p.get("url")
                    if url:
                        declared_routes.append(
                            {
                                "url": url,
                                "display_url": _mask_route_url(url),
                                "group": p.get("group"),
                                "country": p.get("country"),
                            }
                        )
                elif hasattr(p, "url"):
                    declared_routes.append(
                        {
                            "url": p.url,
                            "display_url": _mask_route_url(p.url),
                            "group": getattr(p, "group", None),
                            "country": getattr(getattr(p, "geolocation", None), "country", None),
                        }
                    )

        for route_info in declared_routes:
            url = route_info["url"]
            route_audit[url] = {
                "route_url": url,
                "display_url": route_info.get("display_url", url),
                "group": route_info.get("group"),
                "country": route_info.get("country"),
                "status": "healthy",
                "total_attempts": 0,
                "failures": 0,
                "rate_limits_429": 0,
                "timeouts": 0,
                "latency_samples": [],
                "avg_latency_sec": 0.0,
            }

        # 2. Process tasks if available to tally real runtime performance
        if tasks:
            for t in tasks:
                payload = getattr(t, "payload", None)
                params: Mapping[str, Any] = {}
                if isinstance(payload, dict):
                    params = payload.get("params") or payload
                elif hasattr(payload, "params"):
                    params = payload.params or {}

                proxy_url = None
                if isinstance(payload, dict):
                    proxy_url = payload.get("guest_proxy_url")
                elif hasattr(payload, "guest_proxy_url"):
                    proxy_url = payload.guest_proxy_url

                if not proxy_url and isinstance(params, dict):
                    proxy_url = params.get("guest_proxy_url")

                # If no proxy URL, route is direct
                route_key = proxy_url or DIRECT_ROUTE_KEY

                # If include_direct is False and route is direct, strictly ignore/skip it
                if route_key == DIRECT_ROUTE_KEY and not include_direct:
                    continue

                entry = route_audit.get(route_key)
                if entry is None:
                    entry = {
                        "route_url": route_key,
                        "display_url": _mask_route_url(route_key),
                        "group": None,
                        "country": None,
                        "status": "healthy",
                        "total_attempts": 0,
                        "failures": 0,
                        "rate_limits_429": 0,
                        "timeouts": 0,
                        "latency_samples": [],
                        "avg_latency_sec": 0.0,
                    }
                    route_audit[route_key] = entry

                entry["total_attempts"] += 1

                # Task status & errors
                status = getattr(t, "status", None)
                if isinstance(t, dict):
                    status = t.get("status")
                if status == "failed":
                    entry["failures"] += 1
                    error = str(getattr(t, "error", "") or "").lower()
                    if "429" in error or "rate" in error:
                        entry["rate_limits_429"] += 1
                    if "timeout" in error or "timed out" in error:
                        entry["timeouts"] += 1

                # Task latency
                dur = None
                started = getattr(t, "started_at", None)
                completed = getattr(t, "completed_at", None)
                if isinstance(t, dict):
                    started = t.get("started_at")
                    completed = t.get("completed_at")
                if started and completed:
                    try:
                        from datetime import datetime
                        s_dt = datetime.fromisoformat(str(started))
                        c_dt = datetime.fromisoformat(str(completed))
                        dur = max(0.0, (c_dt - s_dt).total_seconds())
                    except Exception:
                        dur = None
                if dur is not None:
                    entry["latency_samples"].append(dur)

        # 3. Calculate latency averages and anomaly status
        for key, entry in list(route_audit.items()):
            samples = entry["latency_samples"]
            if samples:
                entry["avg_latency_sec"] = round(sum(samples) / len(samples), 3)

            attempts = entry["total_attempts"]
            failures = entry["failures"]
            rate_429 = entry["rate_limits_429"]
            timeouts = entry["timeouts"]
            avg_lat = entry["avg_latency_sec"]

            fail_rate = (failures / attempts * 100.0) if attempts > 0 else 0.0

            if rate_429 >= 3 or failures >= 5 or fail_rate > 30.0:
                entry["status"] = "critical"
                entry["primary_issue"] = (
                    f"Excessive 429 rate limits ({rate_429})"
                    if rate_429 > 0
                    else f"High error rate ({fail_rate:.1f}%)"
                )
            elif timeouts > 0 or avg_lat > 3.5 or failures > 0:
                entry["status"] = "degraded"
                entry["primary_issue"] = (
                    f"Connection timeouts ({timeouts})"
                    if timeouts > 0
                    else f"Elevated latency ({avg_lat:.2f}s)"
                )
            else:
                entry["status"] = "healthy"
                entry["primary_issue"] = "Normal operation"

        # 4. Filter out direct if include_direct is False
        if not include_direct and DIRECT_ROUTE_KEY in route_audit:
            del route_audit[DIRECT_ROUTE_KEY]

        return route_audit


def compile_route_audit(
    job: Any,
    *,
    proxies: Sequence[Any] | None = None,
    tasks: Sequence[Any] | None = None,
    probe_interfaces: bool = False,
) -> dict[str, Any]:
    """Functional wrapper around RouteAuditCompiler.compile."""
    compiler = RouteAuditCompiler()
    return compiler.compile(
        job,
        proxies=proxies,
        tasks=tasks,
        probe_interfaces=probe_interfaces,
    )


def audit_routes(job: Any, **kwargs: Any) -> dict[str, Any]:
    """Alias for compile_route_audit."""
    return compile_route_audit(job, **kwargs)


class ThroughputDiagnosticsCompiler:
    """Compiles throughput bottleneck telemetry, capacity ceilings, and efficiency loss factors."""

    def compile(
        self,
        job: Any,
        *,
        proxies: Sequence[Any] | None = None,
        tasks: Sequence[Any] | None = None,
        worker_pool_size: int = 32,
        default_median_latency: float = 2.1,
    ) -> dict[str, Any]:
        """Compile comprehensive throughput diagnostics for a job."""
        metadata: Mapping[str, Any] = {}
        if hasattr(job, "metadata") and isinstance(job.metadata, dict):
            metadata = job.metadata
        elif isinstance(job, dict):
            metadata = job.get("metadata") or {}

        include_direct = bool(metadata.get("include_direct_connection", False))

        # 1. Resolve proxy capacity
        proxy_capacity = metadata.get("proxy_capacity")
        if not (isinstance(proxy_capacity, (int, float)) and proxy_capacity > 0):
            if proxies:
                total_slots = 0
                for p in proxies:
                    if isinstance(p, dict) and "max_connections" in p:
                        total_slots += int(p.get("max_connections") or 8)
                    elif hasattr(p, "max_connections"):
                        total_slots += int(getattr(p, "max_connections", 8) or 8)
                    else:
                        total_slots += 1
                proxy_capacity = total_slots + (8 if include_direct and total_slots > len(proxies) else (1 if include_direct else 0))
            else:
                proxy_capacity = 107 + (1 if include_direct else 0)

        proxy_capacity = int(proxy_capacity)

        # 2. Extract task stats
        task_stats: Mapping[str, Any] = {}
        if hasattr(job, "task_stats") and isinstance(job.task_stats, dict):
            task_stats = job.task_stats
        elif isinstance(job, dict):
            task_stats = job.get("task_stats") or {}

        completed_tasks = int(task_stats.get("completed") or 0)
        failed_tasks = int(task_stats.get("failed") or 0)
        total_tasks = int(task_stats.get("total") or getattr(job, "task_count", 0) or completed_tasks + failed_tasks)

        # 3. Calculate task latencies and percentiles
        latencies: list[float] = []
        rate_limits_429 = 0
        timeouts = 0

        if tasks:
            for t in tasks:
                error = str(getattr(t, "error", "") or (t.get("error") if isinstance(t, dict) else "") or "").lower()
                if "429" in error or "rate limit" in error:
                    rate_limits_429 += 1
                if "timeout" in error or "timed out" in error:
                    timeouts += 1

                started = getattr(t, "started_at", None) or (t.get("started_at") if isinstance(t, dict) else None)
                completed = getattr(t, "completed_at", None) or (t.get("completed_at") if isinstance(t, dict) else None)
                if started and completed:
                    try:
                        from datetime import datetime
                        s_dt = datetime.fromisoformat(str(started))
                        c_dt = datetime.fromisoformat(str(completed))
                        dur = max(0.01, (c_dt - s_dt).total_seconds())
                        if dur <= 120.0:
                            latencies.append(dur)
                    except Exception:
                        pass

        if latencies:
            sorted_lat = sorted(latencies)
            n = len(sorted_lat)
            p50 = round(sorted_lat[min(int(0.50 * n), n - 1)], 2)
            p90 = round(sorted_lat[min(int(0.90 * n), n - 1)], 2)
            p95 = round(sorted_lat[min(int(0.95 * n), n - 1)], 2)
            p99 = round(sorted_lat[min(int(0.99 * n), n - 1)], 2)
        else:
            p50 = default_median_latency
            p90 = round(default_median_latency * 1.3, 2)
            p95 = round(default_median_latency * 1.6, 2)
            p99 = round(default_median_latency * 2.5, 2)

        effective_p50 = max(0.05, p50)
        runtime_telemetry = metadata.get("runtime_telemetry")
        if not isinstance(runtime_telemetry, Mapping):
            runtime_telemetry = {}
        runtime_task_count = int(runtime_telemetry.get("task_count") or 0)
        runtime_complete = bool(
            runtime_task_count > 0
            and runtime_task_count >= completed_tasks + failed_tasks
        )
        mean_task_duration = (
            float(runtime_telemetry.get("mean_task_duration_ms") or 0) / 1000.0
            if runtime_complete
            else 0.0
        )
        effective_service_time = max(
            0.05, mean_task_duration if mean_task_duration > 0 else effective_p50
        )
        mean_route_active_per_task = (
            float(runtime_telemetry.get("route_active_sum_ms") or 0)
            / runtime_task_count
            / 1000.0
            if runtime_complete and runtime_task_count > 0
            else 0.0
        )
        effective_route_service_time = max(
            0.05,
            mean_route_active_per_task
            if mean_route_active_per_task > 0
            else effective_service_time,
        )

        # 4. Duration and throughput
        duration_sec = 1.0
        explicit_duration = metadata.get("duration_seconds")
        if isinstance(explicit_duration, (int, float)) and explicit_duration > 0:
            duration_sec = float(explicit_duration)
        else:
            started_at = getattr(job, "started_at", None) or getattr(job, "created_at", None)
            completed_at = getattr(job, "completed_at", None) or getattr(job, "last_ran_at", None) or getattr(job, "updated_at", None)
            if started_at and completed_at:
                try:
                    from datetime import datetime
                    s_dt = datetime.fromisoformat(str(started_at))
                    c_dt = datetime.fromisoformat(str(completed_at))
                    dur = (c_dt - s_dt).total_seconds()
                    if dur > 0:
                        duration_sec = dur
                except Exception:
                    pass

        # The compiler receives task counters, not aggregate item counters.
        # Keep this rate explicitly task-based so callers cannot mistake it for
        # item throughput (the WebUI has the same contract for partial pages).
        overall_avg_tasks_per_sec = round(completed_tasks / max(1.0, duration_sec), 1)

        # 5. Theoretical capacities
        args = getattr(job, "args", {}) or {}
        env_concurrency = os.environ.get("ULTIMA_WORKER_CONCURRENCY")
        target_concurrency = int(
            metadata.get("target_concurrency")
            or metadata.get("concurrency")
            or (args.get("target_concurrency") if isinstance(args, dict) else None)
            or (args.get("concurrency") if isinstance(args, dict) else None)
            or env_concurrency
            or max(1, (int(proxy_capacity) + max(1, worker_pool_size) - 1) // max(1, worker_pool_size))
        )
        worker_concurrency_demand = max(1, worker_pool_size * target_concurrency)

        theoretical_proxy_max = round(proxy_capacity / effective_route_service_time, 1)
        theoretical_worker_max = round(worker_concurrency_demand / effective_service_time, 1)
        governing_ceiling = min(theoretical_proxy_max, theoretical_worker_max)
        governing_bottleneck = "proxy_capacity" if theoretical_proxy_max <= theoretical_worker_max else "worker_concurrency"

        # Concurrency according to Little's Law
        littles_law_concurrency = round(min(float(proxy_capacity), overall_avg_tasks_per_sec * effective_service_time), 1)
        active_concurrency = (
            float(runtime_telemetry.get("time_weighted_inflight") or 0)
            if runtime_complete
            else (littles_law_concurrency if completed_tasks > 0 else 0.0)
        )
        active_routes = (
            float(runtime_telemetry.get("time_weighted_active_routes") or 0)
            if runtime_complete
            else active_concurrency
        )
        idle_proxy_routes = max(0, proxy_capacity - int(round(active_routes)))
        proxy_utilization_percent = round((active_routes / max(1, proxy_capacity)) * 100.0, 1)
        worker_utilization_percent = min(100.0, round((active_concurrency / max(1, worker_concurrency_demand)) * 100.0, 1))

        # Latency spread ratio & drag
        latency_spread_ratio = round(p95 / effective_p50, 2)
        tail_latency_drag_percent = (
            min(60.0, round(((p95 - effective_p50) / (p95 + effective_p50)) * 100.0, 1))
            if p95 > effective_p50
            else 0.0
        )

        active_window_seconds = float(runtime_telemetry.get("active_window_ms") or 0) / 1000.0
        active_window_throughput = (
            runtime_task_count / active_window_seconds
            if runtime_complete and active_window_seconds > 0
            else overall_avg_tasks_per_sec
        )
        efficiency_percent = min(100.0, round((active_window_throughput / max(0.1, governing_ceiling)) * 100.0, 1))
        steady_throughput = (
            float(runtime_telemetry.get("steady_throughput") or 0)
            if runtime_complete else 0.0
        )
        steady_mean_duration = (
            float(runtime_telemetry.get("steady_mean_task_duration_ms") or 0) / 1000.0
            if runtime_complete else 0.0
        )
        steady_worker_ceiling = worker_concurrency_demand / max(
            0.05, steady_mean_duration or effective_service_time
        )
        steady_ceiling = min(theoretical_proxy_max, steady_worker_ceiling)
        steady_efficiency_percent = (
            min(100.0, round((steady_throughput / steady_ceiling) * 100.0, 1))
            if steady_throughput > 0 else None
        )
        expected_workers = int(runtime_telemetry.get("expected_workers") or worker_pool_size)
        workers_seen = int(runtime_telemetry.get("workers_seen") or 0)
        ready_ratio = runtime_telemetry.get("ready_nonempty_ratio")
        ready_nonempty_percent = (
            round(float(ready_ratio) * 100.0, 1)
            if ready_ratio is not None
            else None
        )
        ready_sample_count = int(runtime_telemetry.get("ready_sample_count") or 0)
        occupancy_gap_classification = "insufficient_data"
        occupancy_gap_explanation = "Steady-state occupancy telemetry is unavailable."
        if steady_efficiency_percent is not None:
            if steady_efficiency_percent >= 90 and steady_efficiency_percent >= efficiency_percent + 5:
                occupancy_gap_classification = "ramp_drain"
                occupancy_gap_explanation = (
                    f"Steady-state efficiency reached {steady_efficiency_percent}% versus "
                    f"{efficiency_percent}% across the active window; ramp/drain dilution explains the gap."
                )
            elif expected_workers > 0 and workers_seen < expected_workers:
                occupancy_gap_classification = "worker_availability"
                occupancy_gap_explanation = (
                    f"Only {workers_seen} of {expected_workers} expected workers participated."
                )
            elif steady_efficiency_percent < 90:
                occupancy_gap_classification = "insufficient_data"
                ready_observation = (
                    f"The ready queue was non-empty in {ready_nonempty_percent}% of "
                    f"{ready_sample_count} task-completion samples"
                    if ready_nonempty_percent is not None and ready_sample_count > 0
                    else "Ready-queue timing samples are unavailable"
                )
                occupancy_gap_explanation = (
                    f"Steady-state efficiency remained {steady_efficiency_percent}%. {ready_observation}; "
                    "completion-triggered samples do not establish wall-clock queue availability, "
                    "so the remaining occupancy cause cannot be isolated from this telemetry."
                )
            else:
                occupancy_gap_classification = "steady_state_saturated"
                occupancy_gap_explanation = (
                    f"Steady-state efficiency reached {steady_efficiency_percent}% with expected worker participation."
                )

        # 6. Ranked Throughput Loss Factors
        loss_factors: list[dict[str, Any]] = []
        capacity_realization_bottleneck = (
            occupancy_gap_classification
            if occupancy_gap_classification in {
                "routing_stall",
                "claim_refill",
                "worker_availability",
                "queue_starvation",
            }
            else None
        )
        dominant_service_phase: str | None = None
        dominant_service_subphase: str | None = None

        if runtime_complete:
            writer_wait_seconds = float(
                runtime_telemetry.get("mean_writer_ms") or 0
            ) / 1000.0
            writer_queue_seconds = float(
                runtime_telemetry.get("mean_writer_queue_ms") or 0
            ) / 1000.0
            writer_admission_seconds = float(
                runtime_telemetry.get("mean_writer_admission_ms") or 0
            ) / 1000.0
            writer_sql_seconds = (
                float(runtime_telemetry.get("mean_writer_execution_ms") or 0)
                + float(runtime_telemetry.get("mean_writer_commit_ms") or 0)
            ) / 1000.0
            writer_factor = (
                "Writer Queue / Admission Backpressure"
                if max(writer_queue_seconds, writer_admission_seconds)
                > writer_sql_seconds
                else "Database Writer Wait"
            )
            # Every phase that consumes task service time must compete here.
            # Queue / Dispatch Wait is deliberately excluded: it is backlog
            # residency before a worker touches the task, not service time.
            phases = {
                "Worker Setup": float(runtime_telemetry.get("mean_setup_ms") or 0)
                / 1000.0,
                "Handler / Upstream Work": float(
                    runtime_telemetry.get("mean_handler_exclusive_ms")
                    if runtime_telemetry.get("mean_handler_exclusive_ms") is not None
                    else max(
                        0.0,
                        float(runtime_telemetry.get("mean_handler_ms") or 0)
                        - float(runtime_telemetry.get("mean_writer_ms") or 0),
                    )
                ) / 1000.0,
                writer_factor: writer_wait_seconds,
                "Terminal State Update": float(
                    runtime_telemetry.get("mean_post_ms") or 0
                )
                / 1000.0,
                "Release / Publication": float(
                    runtime_telemetry.get("mean_release_ms") or 0
                )
                / 1000.0,
            }
            factor, seconds = max(phases.items(), key=lambda item: item[1])
            dominant_service_phase = factor
            if seconds > 0:
                # "residual" is the uninstrumented remainder, so it is never a
                # nameable subphase; the report table still shows it for
                # coverage. Matches the WebUI's dominant-setup pick.
                setup_subphases = runtime_telemetry.get("setup_subphases")
                dominant_setup_subphase = None
                if isinstance(setup_subphases, Mapping):
                    dominant_setup_subphase = max(
                        (
                            (name, float(entry.get("mean_ms") or 0.0))
                            for name, entry in setup_subphases.items()
                            if name != "residual" and isinstance(entry, Mapping)
                        ),
                        key=lambda item: item[1],
                        default=None,
                    )
                if factor == "Worker Setup" and dominant_setup_subphase:
                    dominant_service_subphase = dominant_setup_subphase[0]
                else:
                    dominant_service_subphase = None
                loss_factors.append({
                    "factor": factor,
                    "severity": "high",
                    "estimated_loss_items_per_sec": 0.0,
                    "description": (
                        f"{factor} is the largest measured phase at {seconds:.2f}s "
                        f"per task"
                        + (
                            f", dominated by {dominant_service_subphase} at "
                            f"{dominant_setup_subphase[1]:.2f}ms mean"
                            if factor == "Worker Setup"
                            and dominant_setup_subphase
                            else ""
                        )
                        + "; this is time attribution, not an additive loss estimate."
                    ),
                    "remedy": "Inspect the measured phase before changing concurrency or route capacity.",
                })

        # Concurrency deficit factor
        if not runtime_complete and proxy_capacity > worker_concurrency_demand:
            deficit_slots = proxy_capacity - worker_concurrency_demand
            loss_items = round(deficit_slots / effective_p50, 1)
            loss_factors.append(
                {
                    "factor": "Worker Concurrency Deficit",
                    "severity": "critical" if deficit_slots > 100 else "high",
                    "estimated_loss_items_per_sec": loss_items,
                    "description": f"Worker concurrency demand ({worker_concurrency_demand} slots: {worker_pool_size} workers × {target_concurrency}) is below proxy fleet capacity ({proxy_capacity} routes), leaving ~{deficit_slots} routes underutilized.",
                    "remedy": f"Increase worker concurrency to at least {(proxy_capacity + worker_pool_size - 1) // worker_pool_size} per worker (or expand worker pool).",
                }
            )
        elif not runtime_complete and idle_proxy_routes > max(10, int(proxy_capacity * 0.25)) and completed_tasks > 0:
            loss_items = round(idle_proxy_routes / effective_p50, 1)
            loss_factors.append(
                {
                    "factor": "Unutilized Proxy Headroom",
                    "severity": "high" if idle_proxy_routes > 50 else "moderate",
                    "estimated_loss_items_per_sec": loss_items,
                    "description": f"{idle_proxy_routes} of {proxy_capacity} proxy routes ({round((idle_proxy_routes / proxy_capacity) * 100, 1)}%) were idle on average during the run.",
                    "remedy": "Increase batch claim size and per-worker concurrent task allocations.",
                }
            )

        # Tail latency drag
        if not runtime_complete and latency_spread_ratio >= 1.5:
            estimated_loss = round(overall_avg_tasks_per_sec * (tail_latency_drag_percent / 100.0), 1)
            loss_factors.append(
                {
                    "factor": "Tail Latency Drag",
                    "severity": "high" if latency_spread_ratio >= 2.5 else "moderate",
                    "estimated_loss_items_per_sec": estimated_loss,
                    "description": f"Tail latency (p95: {p95}s) is {latency_spread_ratio}x higher than median (p50: {p50}s), holding worker slots and connections occupied {latency_spread_ratio}x longer.",
                    "remedy": "Isolate high-latency routes and tighten request timeouts on non-streaming metadata requests.",
                }
            )

        # Rate limits
        if rate_limits_429 > 0:
            observed_tasks = max(1, total_tasks, len(tasks or ()))
            rate_limit_percent = rate_limits_429 / observed_tasks * 100.0
            loss_factors.append(
                {
                    "factor": "Upstream Rate Limiting (HTTP 429)",
                    "severity": (
                        "critical" if rate_limit_percent >= 5.0
                        else "high" if rate_limit_percent >= 1.0
                        else "moderate" if rate_limit_percent >= 0.1
                        else "low"
                    ),
                    "estimated_loss_items_per_sec": 0.0,
                    "description": (
                        f"{rate_limits_429} request(s) were rejected with HTTP 429 "
                        f"({rate_limit_percent:.2f}% observed incidence). No additive "
                        "throughput loss is inferred from the count alone."
                    ),
                    "remedy": (
                        "Inspect per-route attribution before rotating routes; if the 429s "
                        "cluster on identified routes, distribute those requests across "
                        "additional identities or IP subnets."
                    ),
                }
            )

        # Timeouts
        if timeouts > 0:
            observed_tasks = max(1, total_tasks, len(tasks or ()))
            timeout_percent = timeouts / observed_tasks * 100.0
            loss_factors.append(
                {
                    "factor": "Connection Timeouts",
                    "severity": (
                        "critical" if timeout_percent >= 5.0
                        else "high" if timeout_percent >= 1.0
                        else "moderate" if timeout_percent >= 0.1
                        else "low"
                    ),
                    "estimated_loss_items_per_sec": 0.0,
                    "description": (
                        f"{timeouts} request(s) timed out or reset "
                        f"({timeout_percent:.2f}% observed incidence). No additive "
                        "throughput loss is inferred without measured occupied time."
                    ),
                    "remedy": (
                        "Capture per-route timeout attribution and upstream timing before "
                        "pruning routes; investigate only the routes or handler stages that "
                        "the evidence identifies."
                    ),
                }
            )

        recommended_worker_concurrency = max(1, (proxy_capacity + worker_pool_size - 1) // max(1, worker_pool_size))
        recommended_batch_claim_size = max(8, min(32, (recommended_worker_concurrency + 1) // 2))

        return {
            "proxy_capacity": proxy_capacity,
            "worker_pool_size": worker_pool_size,
            "target_concurrency": target_concurrency,
            "worker_concurrency_demand": worker_concurrency_demand,
            "theoretical_proxy_max_items_per_sec": theoretical_proxy_max,
            "theoretical_worker_max_items_per_sec": theoretical_worker_max,
            "governing_ceiling_items_per_sec": governing_ceiling,
            "governing_bottleneck": governing_bottleneck,
            "active_concurrency": active_concurrency,
            "active_routes": active_routes,
            "idle_proxy_routes": idle_proxy_routes,
            "proxy_utilization_percent": proxy_utilization_percent,
            "worker_utilization_percent": worker_utilization_percent,
            "throughput_unit": "tasks",
            "overall_avg_tasks_per_sec": overall_avg_tasks_per_sec,
            # Backward-compatible alias; consumers must inspect throughput_unit.
            "overall_avg_items_per_sec": overall_avg_tasks_per_sec,
            "efficiency_percent": efficiency_percent,
            "active_window_throughput": round(active_window_throughput, 1),
            "steady_state_throughput": round(steady_throughput, 1) if steady_throughput > 0 else None,
            "steady_state_efficiency_percent": steady_efficiency_percent,
            "occupancy_gap_classification": occupancy_gap_classification,
            "occupancy_gap_explanation": occupancy_gap_explanation,
            "capacity_realization_observation": occupancy_gap_classification,
            "capacity_realization_bottleneck": capacity_realization_bottleneck,
            "dominant_service_phase": dominant_service_phase,
            "dominant_service_subphase": dominant_service_subphase,
            "runtime_telemetry_complete": runtime_complete,
            "runtime_telemetry": dict(runtime_telemetry) if runtime_complete else None,
            "duration_sec": round(duration_sec, 1),
            "latency_percentiles": {
                "p50": p50,
                "p90": p90,
                "p95": p95,
                "p99": p99,
                "sample_count": len(latencies),
            },
            "latency_spread_ratio": latency_spread_ratio,
            "tail_latency_drag_percent": tail_latency_drag_percent,
            "loss_factors": loss_factors,
            "recommended_worker_concurrency": recommended_worker_concurrency,
            "recommended_batch_claim_size": recommended_batch_claim_size,
        }


def compile_throughput_diagnostics(
    job: Any,
    *,
    proxies: Sequence[Any] | None = None,
    tasks: Sequence[Any] | None = None,
    worker_pool_size: int = 32,
    default_median_latency: float = 2.1,
) -> dict[str, Any]:
    """Functional wrapper around ThroughputDiagnosticsCompiler.compile."""
    compiler = ThroughputDiagnosticsCompiler()
    return compiler.compile(
        job,
        proxies=proxies,
        tasks=tasks,
        worker_pool_size=worker_pool_size,
        default_median_latency=default_median_latency,
    )
