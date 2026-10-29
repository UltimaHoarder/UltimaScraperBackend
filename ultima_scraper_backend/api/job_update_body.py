"""Read a job request without retaining its full task array in memory."""

from __future__ import annotations

import codecs
import io
import json
import tempfile
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import Any, BinaryIO

import anyio
from anyio.lowlevel import RunVar
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from ..database.threads import run_owned_thread_call


@dataclass
class _UploadUsage:
    requests: int = 0
    reserved_bytes: int = 0


_upload_usage: RunVar[dict[str, _UploadUsage]] = RunVar("job_upload_usage")


@contextmanager
def _admit_upload(
    byte_limit: int, default_inflight_bytes: int | None, resource_prefix: str
):
    from ..database.write_queue import _positive_int_env

    max_bytes = _positive_int_env(
        f"{resource_prefix}_MAX_INFLIGHT_BYTES",
        byte_limit if default_inflight_bytes is None else default_inflight_bytes,
    )
    byte_limit = min(byte_limit, max_bytes)
    max_requests = _positive_int_env(
        f"{resource_prefix}_CONCURRENCY", max_bytes // byte_limit
    )
    pools = _upload_usage.get(None)
    if pools is None:
        pools = {}
        _upload_usage.set(pools)
    usage = pools.setdefault(resource_prefix, _UploadUsage())
    # No await between checking and reserving: all requests on this event loop
    # share the same accounting. Other API processes own separate budgets.
    if usage.requests >= max_requests or usage.reserved_bytes + byte_limit > max_bytes:
        raise HTTPException(
            503, "Job upload capacity is busy; retry after an active upload finishes"
        )
    usage.requests += 1
    usage.reserved_bytes += byte_limit
    try:
        yield byte_limit
    finally:
        usage.requests -= 1
        usage.reserved_bytes -= byte_limit


async def _file_call(function, *args):
    async with anyio.to_thread.current_default_thread_limiter():
        return await run_owned_thread_call(function, *args)


def _invalid(message: str, offset: int = 0) -> RequestValidationError:
    return RequestValidationError(
        [
            {
                "type": "json_invalid",
                "loc": ("body", offset),
                "msg": "JSON decode error",
                "input": {},
                "ctx": {"error": message},
            }
        ]
    )


class _Reader:
    def __init__(self, source: BinaryIO, byte_limit: int):
        self.source = source
        self.byte_limit = byte_limit
        self.buffer = b""
        self.eof = False
        self.decoder = json.JSONDecoder()

    @property
    def position(self) -> int:
        return self.source.tell() - len(self.buffer)

    def more(self, size: int = io.DEFAULT_BUFFER_SIZE) -> None:
        chunk = self.source.read(min(size, self.byte_limit))
        self.buffer += chunk
        self.eof = not chunk

    def grow_value(self) -> None:
        self.check_size(len(self.buffer))
        # Double incomplete values instead of repeatedly parsing every small
        # file block. One extra byte distinguishes an exact bound from overflow.
        self.more(
            min(
                max(len(self.buffer), io.DEFAULT_BUFFER_SIZE),
                self.byte_limit + 1 - len(self.buffer),
            )
        )

    def peek(self) -> bytes:
        while True:
            self.buffer = self.buffer.lstrip(b" \t\r\n")
            if self.buffer or self.eof:
                return self.buffer[:1]
            self.more()

    def take(self, expected: bytes) -> None:
        if self.peek() != expected:
            raise _invalid(f"Expected {expected.decode()!r}", self.position)
        self.buffer = self.buffer[1:]

    def value(self) -> tuple[Any, int]:
        self.peek()
        while True:
            try:
                decoder = codecs.getincrementaldecoder("utf-8")()
                value_text = decoder.decode(self.buffer, final=self.eof)
                value, end = self.decoder.raw_decode(value_text)
            except UnicodeDecodeError as exc:
                raise _invalid("Invalid UTF-8", self.position) from exc
            except json.JSONDecodeError as exc:
                if self.eof:
                    raise _invalid(exc.msg, self.position) from exc
            else:
                # A number at the chunk boundary may continue in the next read.
                if end < len(value_text) or self.eof:
                    if end < len(value_text) and value_text[end] not in " \t\r\n,]}:":
                        if (
                            not self.eof
                            and type(value) in (int, float)
                            and value_text[end] in ".eE+-0123456789"
                        ):
                            self.grow_value()
                            continue
                        raise _invalid("Invalid JSON value separator", self.position)
                    size = len(value_text[:end].encode("utf-8"))
                    self.check_size(size)
                    self.buffer = self.buffer[size:]
                    return value, size
            self.grow_value()

    def check_size(self, size: int) -> None:
        if size > self.byte_limit:
            raise HTTPException(
                413, "One job update value exceeds the configured input byte budget"
            )


class _TaskArray:
    def __init__(self, source, start, count, *, byte_limit, batch_limit, model):
        source.seek(start)
        self.reader = _Reader(source, byte_limit)
        self.remaining = count
        self.batch_limit = batch_limit
        self.model = model
        self.index = 0
        self.pending = None

    def page(self):
        page = []
        occupied = 0
        while self.remaining or self.pending is not None:
            if self.pending is not None:
                value, size = self.pending
                self.pending = None
            else:
                value, size = self.reader.value()
                self.remaining -= 1
                if self.remaining:
                    self.reader.take(b",")
            if page and occupied + size > self.reader.byte_limit:
                self.pending = (value, size)
                break
            try:
                page.append(self.model.model_validate(value))
            except ValidationError as exc:
                raise RequestValidationError(
                    [
                        {**error, "loc": ("body", "tasks", self.index, *error["loc"])}
                        for error in exc.errors()
                    ]
                ) from exc
            self.index += 1
            occupied += size
            if len(page) >= self.batch_limit:
                break
        return page

    async def __aiter__(self):
        while page := await _file_call(self.page):
            for item in page:
                yield item


def _scan(source, byte_limit, batch_limit, request_model, task_model, required_tasks=False):
    source.seek(0)
    reader = _Reader(source, byte_limit)
    reader.take(b"{")
    metadata = {}
    tasks = None
    if reader.peek() != b"}":
        while True:
            key, _ = reader.value()
            if not isinstance(key, str):
                raise _invalid("Object keys must be strings", reader.position)
            reader.take(b":")
            if key == "tasks" and reader.peek() == b"[":
                reader.take(b"[")
                start = reader.position
                count = 0
                if reader.peek() != b"]":
                    while True:
                        reader.value()
                        count += 1
                        if reader.peek() == b"]":
                            break
                        reader.take(b",")
                reader.take(b"]")
                tasks = (start, count)
                metadata["tasks"] = [] if required_tasks else None
            else:
                value, _ = reader.value()
                if key in request_model.model_fields:
                    metadata[key] = value
                if key == "tasks":
                    tasks = None
            if reader.peek() == b"}":
                break
            reader.take(b",")
    reader.take(b"}")
    if reader.peek():
        raise _invalid("Extra data after job update", reader.position)
    try:
        request = request_model.model_validate(metadata)
    except ValidationError as exc:
        raise RequestValidationError(
            [{**error, "loc": ("body", *error["loc"])} for error in exc.errors()]
        ) from exc
    return request, (
        _TaskArray(
            source,
            *tasks,
            byte_limit=byte_limit,
            batch_limit=batch_limit,
            model=task_model,
        )
        if tasks is not None
        else None
    )


@asynccontextmanager
async def read_job_update(
    request: Request,
    *,
    byte_limit: int,
    batch_limit: int,
    request_model,
    task_model,
    default_inflight_bytes: int | None = None,
    resource_prefix: str = "ULTIMA_MPS_UPLOAD",
    required_tasks: bool = False,
):
    # TemporaryFile follows the deployment's TMPDIR. Thread operations are
    # shielded until completion so cancellation cannot close an in-use file.
    with (
        _admit_upload(
            byte_limit, default_inflight_bytes, resource_prefix
        ) as byte_limit,
        tempfile.TemporaryFile(mode="w+b", prefix="usr-job-update-") as source,
    ):
        async for chunk in request.stream():
            await _file_call(source.write, chunk)
        parsed = await _file_call(
            _scan,
            source,
            byte_limit,
            batch_limit,
            request_model,
            task_model,
            required_tasks,
        )
        yield parsed


def request_body_schema(model) -> dict[str, Any]:
    """Keep the existing HTTP contract visible when parsing the body manually."""
    schema = model.model_json_schema()
    definitions = schema.pop("$defs", {})

    def inline(value):
        if isinstance(value, dict):
            if "$ref" in value:
                return inline(definitions[value["$ref"].rsplit("/", 1)[1]])
            return {key: inline(item) for key, item in value.items()}
        if isinstance(value, list):
            return [inline(item) for item in value]
        return value

    return {
        "required": True,
        "content": {"application/json": {"schema": inline(schema)}},
    }
