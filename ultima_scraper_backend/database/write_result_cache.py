"""Bounded Redis delivery caches; PostgreSQL owns the durable outcomes."""

from __future__ import annotations

from typing import Any
from ..redis_channels import KEY_PREFIX

RESULT_INDEX = f"{KEY_PREFIX}:database:write:results:index"
RESULT_SIZES = f"{KEY_PREFIX}:database:write:results:sizes"
RESULT_BYTES = f"{KEY_PREFIX}:database:write:results:bytes"
DEAD_BYTES = f"{KEY_PREFIX}:database:writes:dead:bytes"

_RESULT = """
local id, payload = ARGV[1], ARGV[2]
local max_count, max_bytes = tonumber(ARGV[3]), tonumber(ARGV[4])
local total = tonumber(redis.call('GET', KEYS[5]) or '0')
local previous = tonumber(redis.call('HGET', KEYS[4], id) or '0')
redis.call('ZREM', KEYS[3], id)
redis.call('HDEL', KEYS[4], id)
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[6])
total = math.max(0, total - previous)
local size = string.len(payload)
local work = 0
while work < tonumber(ARGV[6]) and
    (redis.call('ZCARD', KEYS[3]) >= max_count or total + size > max_bytes) do
    local first = redis.call('ZRANGE', KEYS[3], 0, 0)
    if #first == 0 then break end
    local old_id = first[1]
    local old_size = tonumber(redis.call('HGET', KEYS[4], old_id) or '0')
    redis.call('DEL', ARGV[7] .. old_id)
    redis.call('DEL', ARGV[8] .. old_id)
    redis.call('ZREM', KEYS[3], old_id)
    redis.call('HDEL', KEYS[4], old_id)
    total = math.max(0, total - old_size)
    work = work + 1
end
local cached = 0
if redis.call('ZCARD', KEYS[3]) < max_count and total + size <= max_bytes then
    redis.call('SET', KEYS[1], payload, 'EX', ARGV[5])
    local now = redis.call('TIME')
    local now_us = tonumber(now[1]) * 1000000 + tonumber(now[2])
    redis.call('SET', KEYS[6], now_us / 1000, 'EX', ARGV[5])
    redis.call('ZADD', KEYS[3], now_us, id)
    redis.call('HSET', KEYS[4], id, size)
    total = total + size
    cached = 1
end
redis.call('SET', KEYS[5], total)
-- A missed cache insertion is safe only after the PostgreSQL receipt commits.
redis.call('DEL', KEYS[2])
return cached
"""

_DEAD = """
local previous = redis.call('GET', KEYS[2])
if not previous and redis.call('XLEN', KEYS[1]) > 0 then
    -- Old dead letters have no receipts or byte accounting. Keep their evidence
    -- until a bounded archival pass has copied it into PostgreSQL.
    return 0
end
local max_count, max_bytes = tonumber(ARGV[1]), tonumber(ARGV[2])
local fields, size = {}, 0
for i = 4, #ARGV do
    fields[#fields + 1] = ARGV[i]
    size = size + string.len(ARGV[i])
end
local total, work = tonumber(previous or '0'), 0
while work < tonumber(ARGV[3]) and
    (redis.call('XLEN', KEYS[1]) >= max_count or total + size > max_bytes) do
    local first = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)
    if #first == 0 then break end
    local old_size = 0
    for _, value in ipairs(first[1][2]) do old_size = old_size + string.len(value) end
    redis.call('XDEL', KEYS[1], first[1][1])
    total = math.max(0, total - old_size)
    work = work + 1
end
local cached = 0
if redis.call('XLEN', KEYS[1]) < max_count and total + size <= max_bytes then
    redis.call('XADD', KEYS[1], '*', unpack(fields))
    total = total + size
    cached = 1
end
redis.call('SET', KEYS[2], total)
return cached
"""


async def cache_result(
    client: Any,
    pipe: Any,
    result: Any,
    *,
    max_entries: int,
    max_bytes: int,
    eviction_batch_size: int,
) -> None:
    from .write_queue import DatabaseWriteClient

    await client.register_script(_RESULT)(
        keys=[
            DatabaseWriteClient.result_key(result.command_id),
            DatabaseWriteClient.submitted_key(result.command_id),
            RESULT_INDEX,
            RESULT_SIZES,
            RESULT_BYTES,
            DatabaseWriteClient.result_available_key(result.command_id),
        ],
        args=[
            result.command_id,
            result.to_json(),
            max_entries,
            max_bytes,
            DatabaseWriteClient.RESULT_TTL_SECONDS,
            eviction_batch_size,
            DatabaseWriteClient.RESULT_KEY.replace("{command_id}", ""),
            DatabaseWriteClient.RESULT_AVAILABLE_KEY.replace("{command_id}", ""),
        ],
        client=pipe,
    )


async def cache_dead_letter(
    client: Any,
    pipe: Any,
    stream: str,
    fields: dict[str, str],
    *,
    max_entries: int,
    max_bytes: int,
    eviction_batch_size: int,
) -> None:
    await client.register_script(_DEAD)(
        keys=[stream, DEAD_BYTES],
        args=[
            max_entries,
            max_bytes,
            eviction_batch_size,
            *(item for pair in fields.items() for item in pair),
        ],
        client=pipe,
    )


_LEGACY_PEEK = """
if redis.call('EXISTS', KEYS[2]) == 1 then return {} end
local first = redis.call('XRANGE', KEYS[1], '-', '+', 'COUNT', 1)
if #first == 0 then
    redis.call('SET', KEYS[2], 0)
    return {}
end
local size = 0
for _, value in ipairs(first[1][2]) do size = size + string.len(value) end
if size > tonumber(ARGV[2]) then
    return redis.error_reply('Legacy dead letter exceeds the configured writer read byte budget')
end
if size > tonumber(ARGV[1]) then return {} end
return {first[1], size}
"""


async def legacy_dead_letter(
    client: Any, stream: str, remaining: int, maximum: int
) -> Any:
    return await client.eval(_LEGACY_PEEK, 2, stream, DEAD_BYTES, remaining, maximum)
