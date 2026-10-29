"""Adaptive user-ID boundary discovery and population estimation for missing or empty databases."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class BoundaryDiscoveryConfig(BaseModel):
    """Configuration and limits for adaptive user-ID boundary discovery."""

    probe_multiplier: float = Field(
        default=10.0,
        ge=1.5,
        le=100.0,
        description="Multiplier for exponential probing phase (e.g. 10x).",
    )
    min_id: int = Field(
        default=1,
        ge=1,
        description="Minimum user ID to begin probing from.",
    )
    known_max_id: int | None = Field(
        default=None,
        ge=1,
        description="Highest known valid user ID. When provided, exponential probing starts here.",
    )
    known_min_id: int | None = Field(
        default=None,
        ge=1,
        description="Lowest known valid user ID.",
    )
    max_id_ceiling: int = Field(
        default=100_000_000_000,
        ge=10,
        description="Maximum safety ceiling for ID probing.",
    )
    max_probes: int = Field(
        default=60,
        ge=5,
        le=500,
        description="Hard ceiling on total network lookups performed during discovery.",
    )
    samples_per_step: int = Field(
        default=3,
        ge=1,
        le=20,
        description=(
            "Number of probe IDs tested per exponential decade/bracket. "
            "Prevents a single missing ID from falsely halting the search."
        ),
    )
    refinement_steps: int = Field(
        default=8,
        ge=1,
        le=30,
        description="Number of binary/bisection refinement steps.",
    )
    samples_per_refinement: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Sample checks performed per refinement step.",
    )
    consecutive_empty_brackets: int = Field(
        default=2,
        ge=1,
        le=5,
        description=(
            "Consecutive empty decades with zero valid users required "
            "before concluding the upper limit is reached."
        ),
    )
    confidence_level: float = Field(
        default=0.95,
        ge=0.50,
        le=0.999,
        description="Confidence level for estimation intervals (e.g. 0.95 for 95%).",
    )
    target_margin_of_error: float = Field(
        default=0.05,
        gt=0.0,
        le=0.50,
        description="Target margin of error for adaptive population density sampling.",
    )
    adaptive_sample_batch: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Batch size for adaptive sampling iterations.",
    )


class ProbeObservation(BaseModel):
    """A single ID probe observation."""

    user_id: int
    is_valid: bool
    phase: str
    timestamp: float = Field(default_factory=time.monotonic)


class MPSBoundaryDiscoveryResult(BaseModel):
    """Result of adaptive boundary discovery and population estimation."""

    site: str
    discovered_start_id: int
    discovered_end_id: int
    highest_observed_id: int | None
    lowest_observed_id: int | None
    estimated_total_users: int
    population_confidence_interval: tuple[int, int]
    estimated_density: float
    density_margin_of_error: float
    confidence_level: float
    highest_id_confidence_interval: tuple[int, int]
    total_probes_used: int
    valid_probes_count: int
    warnings: list[str] = Field(default_factory=list)
    observations: list[ProbeObservation] = Field(default_factory=list)


def is_adaptive_boundary_discovery_enabled(
    config: Any | None = None,
    request: Any | None = None,
) -> bool:
    """Check if adaptive boundary discovery is enabled (enabled by default, no flags required)."""
    return not (request is not None and getattr(request, "enable_adaptive_discovery", None) is False)


def get_z_score(confidence: float) -> float:
    """Approximate two-tailed normal quantile (z-score) for given confidence level."""
    if confidence >= 0.999:
        return 3.291
    if confidence >= 0.99:
        return 2.576
    if confidence >= 0.98:
        return 2.326
    if confidence >= 0.95:
        return 1.960
    if confidence >= 0.90:
        return 1.645
    if confidence >= 0.85:
        return 1.440
    if confidence >= 0.80:
        return 1.282
    return 1.960


def calculate_german_tank_estimate(
    highest_observed: int,
    k_samples: int,
    upper_bound: int,
) -> int:
    """Minimum-variance unbiased estimator (MVUE) for maximum in uniform order statistics.

    Formula: N_hat = m + (m - k) / k = m * (k + 1) / k - 1
    Capped by the established upper bound.
    """
    if k_samples <= 0 or highest_observed <= 0:
        return max(1, highest_observed)
    estimate = round(highest_observed + (highest_observed - k_samples) / k_samples)
    return min(upper_bound, max(highest_observed, estimate))


def calculate_highest_id_confidence_interval(
    highest_observed: int,
    k_samples: int,
    upper_bound: int,
    confidence_level: float = 0.95,
) -> tuple[int, int]:
    """Calculate [lower, upper] confidence interval for highest assigned ID.

    Under order statistics: P(M <= m) = (m / N)^k = alpha => N_upper = m / (alpha^(1/k)).
    """
    if highest_observed <= 0:
        return (0, 0)
    lower = highest_observed
    if k_samples <= 0:
        return (lower, upper_bound)
    alpha = max(0.001, min(0.50, 1.0 - confidence_level))
    try:
        factor = alpha ** (1.0 / k_samples)
        calculated_upper = round(highest_observed / factor) if factor > 0 else upper_bound
        upper = min(upper_bound, max(highest_observed, calculated_upper))
    except (OverflowError, ZeroDivisionError):
        upper = upper_bound
    return (lower, upper)


def calculate_wilson_density_and_population(
    total_range_size: int,
    valid_count: int,
    sample_count: int,
    confidence_level: float = 0.95,
) -> tuple[float, float, int, tuple[int, int]]:
    """Calculate Wilson score interval for density and project to total user population.

    Returns:
        (point_density, margin_of_error, point_population, (pop_lower, pop_upper))
    """
    if total_range_size <= 0:
        return (0.0, 0.0, 0, (0, 0))
    if sample_count <= 0:
        return (0.0, 1.0, 0, (0, total_range_size))

    p_hat = valid_count / sample_count
    z = get_z_score(confidence_level)
    z2 = z * z
    n = float(sample_count)

    # Wilson score interval center and half-width
    denom = 1.0 + z2 / n
    center = (p_hat + z2 / (2.0 * n)) / denom
    radicand = (p_hat * (1.0 - p_hat) / n) + (z2 / (4.0 * n * n))
    half_width = (z / denom) * math.sqrt(max(0.0, radicand))

    # Finite Population Correction (FPC) factor: sqrt(1 - n / N)
    if total_range_size > sample_count:
        fpc = math.sqrt(max(0.0, 1.0 - (n / float(total_range_size))))
        half_width *= fpc

    density_lower = max(0.0, min(1.0, center - half_width))
    density_upper = max(0.0, min(1.0, center + half_width))

    point_pop = min(total_range_size, max(0, round(total_range_size * p_hat)))
    pop_lower = min(total_range_size, max(0, round(total_range_size * density_lower)))
    pop_upper = min(total_range_size, max(pop_lower, round(total_range_size * density_upper)))

    return (
        round(p_hat, 4),
        round(half_width, 4),
        point_pop,
        (pop_lower, pop_upper),
    )


class AdaptiveBoundaryDiscoverer:
    """Orchestrates exponential probing, bracket refinement, and adaptive sampling."""

    def __init__(
        self,
        lookup_fn: Callable[[int], Awaitable[bool]],
        config: BoundaryDiscoveryConfig | None = None,
        site: str = "OnlyFans",
        on_probe: Callable[[ProbeObservation, int, int | None], Awaitable[None]] | None = None,
        on_phase: Callable[[str, str], Awaitable[None]] | None = None,
    ) -> None:
        self.lookup_fn = lookup_fn
        self.config = config or BoundaryDiscoveryConfig()
        self.site = site
        self.on_probe = on_probe
        self.on_phase = on_phase
        self._lock = asyncio.Lock()
        self.probed_ids: dict[int, bool] = {}
        self.observations: list[ProbeObservation] = []
        self.valid_ids: set[int] = set()
        self.warnings: list[str] = []

        if self.config.known_min_id is not None and self.config.known_min_id > 0:
            self.valid_ids.add(self.config.known_min_id)
            self.probed_ids[self.config.known_min_id] = True
        if self.config.known_max_id is not None and self.config.known_max_id > 0:
            self.valid_ids.add(self.config.known_max_id)
            self.probed_ids[self.config.known_max_id] = True

    @property
    def total_probes(self) -> int:
        return len(self.probed_ids)

    async def _probe(self, user_id: int, phase: str) -> bool:
        """Probe a single user ID, caching results and respecting the probe budget."""
        async with self._lock:
            if user_id in self.probed_ids:
                return self.probed_ids[user_id]
            if len(self.probed_ids) >= self.config.max_probes:
                return False

        try:
            is_valid = await self.lookup_fn(user_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[%s] Probe failed for ID %s: %s", self.site, user_id, exc)
            is_valid = False

        async with self._lock:
            self.probed_ids[user_id] = is_valid
            obs = ProbeObservation(user_id=user_id, is_valid=is_valid, phase=phase)
            self.observations.append(obs)
            if is_valid:
                self.valid_ids.add(user_id)
            total = len(self.probed_ids)
            highest = max(self.valid_ids) if self.valid_ids else None

        if self.on_probe is not None:
            try:
                await self.on_probe(obs, total, highest)
            except Exception:
                logger.debug("on_probe callback raised exception", exc_info=True)
        return is_valid

    async def _probe_with_neighbors(
        self,
        user_id: int,
        max_id: int,
        phase: str,
        max_neighbors: int = 2,
    ) -> bool:
        """Probe an ID, checking small neighbor offsets if invalid to absorb holes."""
        hit = await self._probe(user_id, phase=phase)
        if hit:
            return True
        for offset in range(1, max_neighbors + 1):
            cand = user_id + offset
            if cand > max_id or self.total_probes >= self.config.max_probes:
                break
            if await self._probe(cand, phase=phase):
                return True
        return False

    def _generate_bracket_candidates(self, start_id: int, end_id: int, count: int) -> list[int]:
        """Generate representative probe IDs within a range to account for missing IDs."""
        if start_id >= end_id:
            return [start_id]
        span = end_id - start_id + 1
        if span <= count:
            return list(range(start_id, end_id + 1))

        candidates: list[int] = []

        # If this looks like an exponential decade bracket (e.g. 1..9, 10..99, 100..999)
        # include the canonical 1-2-5 series in ascending order (1x, 2x, 5x)
        is_power_of_ten = False
        temp = start_id
        while temp > 1 and temp % 10 == 0:
            temp //= 10
        if temp == 1 and end_id >= start_id * 5:
            is_power_of_ten = True

        if is_power_of_ten:
            for factor in (1.0, 2.0, 5.0):
                val = int(start_id * factor)
                if start_id <= val <= end_id and val not in candidates:
                    candidates.append(val)
        else:
            # Include start_id first when exploring custom ranges, followed by stratified points
            candidates.append(start_id)
            step = (end_id - start_id) / (count + 1)
            for i in range(1, count + 1):
                cand = round(start_id + i * step)
                if start_id <= cand <= end_id and cand not in candidates:
                    candidates.append(cand)

        # Fallback if candidates is empty
        if not candidates:
            candidates = [start_id, (start_id + end_id) // 2, end_id]

        if start_id not in candidates:
            candidates.insert(0, start_id)

        return candidates[: max(count, 3)]

    async def run_exponential_probing(self) -> tuple[int | None, int]:
        """Execute exponential probing (e.g. x10) to find initial presence and upper envelope.

        Failed lookups do NOT halt the search; multiple samples per bracket and
        consecutive empty brackets are required before stopping.

        Returns:
            (highest_observed_id, upper_envelope_id)
        """
        if self.on_phase is not None:
            try:
                await self.on_phase("exponential", "Exponential probing: finding candidate brackets")
            except Exception:
                logger.debug("on_phase callback raised exception", exc_info=True)

        ceiling = self.config.max_id_ceiling
        consecutive_empty = 0
        upper_envelope = ceiling
        probes_reserved = min(8, max(2, int(self.config.max_probes * 0.15)))
        exp_limit = max(1, self.config.max_probes - probes_reserved)

        has_known_frontier = (
            self.config.known_max_id is not None and self.config.known_max_id > 0
        )

        if has_known_frontier:
            frontier = self.config.known_max_id
            # Start with a sensible delta scaled to the known frontier (e.g. 1k for 50k, 10k for 1M)
            delta = max(1_000, min(50_000, frontier // 50))
            curr = frontier

            while curr < ceiling and self.total_probes < exp_limit:
                next_step = min(ceiling, curr + delta)
                candidates = self._generate_bracket_candidates(
                    curr + 1, next_step, self.config.samples_per_step
                )

                bracket_hit = False
                for cand in candidates:
                    if self.total_probes >= exp_limit:
                        break
                    hit = await self._probe_with_neighbors(
                        cand, max_id=next_step, phase="exponential"
                    )
                    if hit:
                        bracket_hit = True
                        break

                if bracket_hit:
                    consecutive_empty = 0
                    curr = max(self.valid_ids) if self.valid_ids else next_step
                    delta = min(ceiling - curr, max(delta * 2, int(delta * 2.5)))
                else:
                    consecutive_empty += 1
                    if (
                        bool(self.valid_ids)
                        and consecutive_empty >= self.config.consecutive_empty_brackets
                    ):
                        upper_envelope = next_step
                        break
                    curr = next_step
                    delta = min(ceiling - curr, max(delta * 2, int(delta * 2.5)))

                if next_step >= ceiling:
                    upper_envelope = ceiling
                    break
        else:
            curr = max(1, self.config.min_id)
            multiplier = getattr(self.config, "probe_multiplier", 10.0) or 10.0

            while curr <= ceiling and self.total_probes < exp_limit:
                next_step = min(ceiling, max(curr + 1, int(curr * multiplier)))
                candidates = self._generate_bracket_candidates(
                    curr, next_step - 1, self.config.samples_per_step
                )

                bracket_hit = False
                for cand in candidates:
                    if self.total_probes >= exp_limit:
                        break
                    hit = await self._probe_with_neighbors(
                        cand, max_id=next_step - 1, phase="exponential"
                    )
                    if hit:
                        bracket_hit = True
                        break

                if bracket_hit:
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    if (
                        bool(self.valid_ids)
                        and consecutive_empty >= self.config.consecutive_empty_brackets
                    ):
                        upper_envelope = next_step
                        break

                if next_step >= ceiling:
                    upper_envelope = ceiling
                    break
                curr = next_step

        highest_observed = max(self.valid_ids) if self.valid_ids else None
        return highest_observed, upper_envelope

    async def run_refinement(self, lower_bound: int, upper_bound: int) -> tuple[int, int]:
        """Refine the search range between the highest observed hit and the upper envelope.

        Uses bisection with multi-probe verification to account for non-sequential IDs.
        """
        if self.on_phase is not None:
            try:
                await self.on_phase("refinement", "Bisection refinement: narrowing boundary range")
            except Exception:
                logger.debug("on_phase callback raised exception", exc_info=True)

        left = lower_bound
        right = upper_bound
        steps = self.config.refinement_steps
        probes_reserved = min(8, max(2, int(self.config.max_probes * 0.15)))
        ref_limit = max(self.total_probes, self.config.max_probes - probes_reserved)

        while steps > 0 and (right - left) > 100 and self.total_probes < ref_limit:
            steps -= 1
            mid = (left + right) // 2

            # Probe around mid to determine if valid users exist at or above mid
            hit = await self._probe_with_neighbors(
                mid,
                max_id=right,
                phase="refinement",
                max_neighbors=min(2, max(1, self.config.samples_per_refinement - 1)),
            )
            if hit:
                valid_at_or_above = [uid for uid in self.valid_ids if uid >= mid]
                left = max(valid_at_or_above) if valid_at_or_above else mid
            else:
                right = mid

        highest_observed = max(self.valid_ids) if self.valid_ids else lower_bound
        right = max(highest_observed, right)
        return highest_observed, right

    async def run_adaptive_sampling(
        self,
        range_start: int,
        estimated_max_id: int,
    ) -> tuple[float, float, int, tuple[int, int]]:
        """Adaptive sampling across the ID span to achieve target confidence on population."""
        if self.on_phase is not None:
            try:
                await self.on_phase("sampling", "Adaptive sampling: estimating active population density")
            except Exception:
                logger.debug("on_phase callback raised exception", exc_info=True)

        span = max(1, estimated_max_id - range_start + 1)

        # Draw independent, uniformly random sample points to avoid ascertainment bias
        sample_batch_size = max(10, self.config.adaptive_sample_batch)
        sampled_uids: list[int] = []
        valid_count = 0

        # Draw initial random sample concurrently
        budget_left = max(0, self.config.max_probes - self.total_probes)
        initial_sample_size = min(30, max(sample_batch_size, int(self.config.max_probes * 0.35)))
        target_initial = min(span, min(budget_left, initial_sample_size))
        if target_initial > 0:
            initial_candidates: list[int] = []
            attempts = 0
            seen_cand: set[int] = set()
            while len(initial_candidates) < target_initial and attempts < target_initial * 10:
                attempts += 1
                cand_id = random.randint(range_start, estimated_max_id)
                if cand_id not in seen_cand and cand_id not in self.probed_ids:
                    seen_cand.add(cand_id)
                    initial_candidates.append(cand_id)

            sem = asyncio.Semaphore(min(5, max(2, len(initial_candidates))))

            async def _probe_sample(uid: int) -> tuple[int, bool]:
                async with sem:
                    h = await self._probe(uid, phase="adaptive_sampling")
                    return uid, h

            sample_results = await asyncio.gather(
                *[_probe_sample(uid) for uid in initial_candidates]
            )
            for uid, h in sample_results:
                sampled_uids.append(uid)
                if h:
                    valid_count += 1

        # Fallback to all in-range probes if budget was exhausted before random sampling
        if not sampled_uids:
            in_range_probes = [
                uid for uid in self.probed_ids if range_start <= uid <= estimated_max_id
            ]
            sampled_uids = in_range_probes
            valid_count = sum(1 for uid in in_range_probes if self.probed_ids[uid])

        density, moe, pop, pop_ci = calculate_wilson_density_and_population(
            total_range_size=span,
            valid_count=valid_count,
            sample_count=len(sampled_uids),
            confidence_level=self.config.confidence_level,
        )

        # Adaptively draw more samples concurrently if margin of error > target and budget remains
        while moe > self.config.target_margin_of_error and self.total_probes < self.config.max_probes:
            next_batch_size = min(
                sample_batch_size,
                self.config.max_probes - self.total_probes,
            )
            if next_batch_size <= 0:
                break
            batch_candidates: list[int] = []
            attempts = 0
            while len(batch_candidates) < next_batch_size and attempts < next_batch_size * 10:
                attempts += 1
                uid = random.randint(range_start, estimated_max_id)
                if uid not in self.probed_ids and uid not in batch_candidates:
                    batch_candidates.append(uid)
            if not batch_candidates:
                break

            batch_sem = asyncio.Semaphore(min(4, len(batch_candidates)))

            async def _probe_batch_item(
                u: int, sem: asyncio.Semaphore = batch_sem
            ) -> tuple[int, bool]:
                async with sem:
                    h = await self._probe(u, phase="adaptive_sampling")
                    return u, h

            batch_results = await asyncio.gather(
                *[_probe_batch_item(u) for u in batch_candidates]
            )
            for u, h in batch_results:
                sampled_uids.append(u)
                if h:
                    valid_count += 1

            density, moe, pop, pop_ci = calculate_wilson_density_and_population(
                total_range_size=span,
                valid_count=valid_count,
                sample_count=len(sampled_uids),
                confidence_level=self.config.confidence_level,
            )

        return density, moe, pop, pop_ci

    async def run(self) -> MPSBoundaryDiscoveryResult:
        """Run complete boundary discovery pipeline."""
        highest_obs, upper_envelope = await self.run_exponential_probing()

        if highest_obs is None:
            self.warnings.append(
                f"No valid user IDs found for site '{self.site}' up to ceiling {self.config.max_id_ceiling}."
            )
            return MPSBoundaryDiscoveryResult(
                site=self.site,
                discovered_start_id=self.config.min_id,
                discovered_end_id=self.config.min_id,
                highest_observed_id=None,
                lowest_observed_id=None,
                estimated_total_users=0,
                population_confidence_interval=(0, 0),
                estimated_density=0.0,
                density_margin_of_error=0.0,
                confidence_level=self.config.confidence_level,
                highest_id_confidence_interval=(0, 0),
                total_probes_used=self.total_probes,
                valid_probes_count=0,
                warnings=self.warnings,
                observations=self.observations,
            )

        # Refine between highest observed and upper envelope
        refined_highest, refined_upper = await self.run_refinement(highest_obs, upper_envelope)

        # Estimate highest valid ID using German Tank / order statistics
        k_valid = len(self.valid_ids)
        if (
            self.config.known_max_id is not None
            and refined_highest <= self.config.known_max_id
        ):
            estimated_max_id = self.config.known_max_id
            highest_id_ci = (self.config.known_max_id, self.config.known_max_id)
        else:
            estimated_max_id = calculate_german_tank_estimate(
                refined_highest, k_valid, refined_upper
            )
            highest_id_ci = calculate_highest_id_confidence_interval(
                refined_highest, k_valid, refined_upper, self.config.confidence_level
            )

        lowest_obs = min(self.valid_ids) if self.valid_ids else self.config.min_id
        start_id = min(self.config.known_min_id or self.config.min_id, lowest_obs)

        # Adaptive sampling for total population and density
        density, moe, pop, pop_ci = await self.run_adaptive_sampling(
            start_id, estimated_max_id
        )

        return MPSBoundaryDiscoveryResult(
            site=self.site,
            discovered_start_id=start_id,
            discovered_end_id=estimated_max_id,
            highest_observed_id=refined_highest,
            lowest_observed_id=lowest_obs,
            estimated_total_users=pop,
            population_confidence_interval=pop_ci,
            estimated_density=density,
            density_margin_of_error=moe,
            confidence_level=self.config.confidence_level,
            highest_id_confidence_interval=highest_id_ci,
            total_probes_used=self.total_probes,
            valid_probes_count=k_valid,
            warnings=self.warnings,
            observations=self.observations,
        )


async def discover_user_id_boundaries(
    site: str,
    lookup_fn: Callable[[int], Awaitable[bool]] | None = None,
    backend_manager: Any | None = None,
    config: BoundaryDiscoveryConfig | None = None,
    on_probe: Callable[[ProbeObservation, int, int | None], Awaitable[None]] | None = None,
    on_phase: Callable[[str, str], Awaitable[None]] | None = None,
) -> MPSBoundaryDiscoveryResult:
    """Convenience helper to run adaptive boundary discovery."""
    if config is None:
        config = BoundaryDiscoveryConfig()

    if lookup_fn is None:
        if backend_manager is None:
            raise ValueError("Either lookup_fn or backend_manager must be provided")
        site_key = site.strip().lower()

        async def _default_lookup(user_id: int) -> bool:
            db_mgr = (
                backend_manager.get_site_db_manager(site_key)
                if hasattr(backend_manager, "get_site_db_manager")
                else None
            )
            if db_mgr is not None and hasattr(db_mgr, "get_user"):
                try:
                    user_or_coro = db_mgr.get_user(user_id)
                    db_user = (
                        await user_or_coro
                        if inspect.isawaitable(user_or_coro)
                        else user_or_coro
                    )
                    if db_user is not None and getattr(db_user, "active", True):
                        return True
                except Exception as db_exc:
                    logger.debug("Database user lookup for ID %s failed: %s", user_id, db_exc)

            try:
                res = await backend_manager.get_user(site_key, user_id)
                return bool(res.success)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Backend user lookup for ID %s failed: %s", user_id, exc)
                return False

        lookup_fn = _default_lookup

    discoverer = AdaptiveBoundaryDiscoverer(
        lookup_fn=lookup_fn,
        config=config,
        site=site,
        on_probe=on_probe,
        on_phase=on_phase,
    )
    return await discoverer.run()
