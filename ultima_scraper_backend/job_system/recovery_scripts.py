"""Atomic recovery guards; task JSON is compared verbatim, never Lua-reencoded."""

COMPARE_DELETE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""

COMPARE_EXPIRE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('EXPIRE', KEYS[1], ARGV[2])
"""

DELETE_STALE_EXEC = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('TTL', KEYS[1]) > 60 then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
return redis.call('DEL', KEYS[1])
"""

DELETE_STALE_DEDUP = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
return redis.call('DEL', KEYS[1])
"""

_REMOVE_MEMBER = """
local function remove_member(key, member, marker)
    local removed = redis.call('SREM', key, member)
    if redis.call('SCARD', key) == 1 and redis.call('SISMEMBER', key, marker) == 1 then
        redis.call('DEL', key)
    end
    return removed
end
"""

DROP_STALE_DEFERRED = _REMOVE_MEMBER + """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -2 end
if (redis.call('GET', KEYS[2]) or '') ~= ARGV[2] then return 0 end
return remove_member(KEYS[3], ARGV[3], ARGV[4])
"""

REQUEUE_DEFERRED = _REMOVE_MEMBER + """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -2 end
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
local status = redis.call('HGET', KEYS[3], 'status')
if status == 'completed' or status == 'failed' or status == 'cancelled' then
    remove_member(KEYS[5], ARGV[5], ARGV[6])
    return 0
end
if not status then return 0 end
local backend = redis.call('HGET', KEYS[3], 'backend_id')
if not backend or backend == '' then backend = ARGV[8] end
if backend ~= ARGV[7] then return 0 end
if redis.call('EXISTS', KEYS[6]) == 1 or redis.call('EXISTS', KEYS[7]) == 1 then return 0 end
if redis.call('SISMEMBER', KEYS[5], ARGV[5]) ~= 1 then return 0 end

redis.call('SET', KEYS[2], ARGV[3], 'KEEPTTL')
local added = redis.call('SADD', KEYS[4], ARGV[4])
if added > 0 then redis.call('HINCRBY', KEYS[3], 'ready_tasks', added) end
local deferred = tonumber(redis.call('HGET', KEYS[3], 'deferred_tasks') or '0')
if deferred > 0 then redis.call('HINCRBY', KEYS[3], 'deferred_tasks', -1) end
redis.call('HSET', KEYS[3], 'updated_at', ARGV[9])
remove_member(KEYS[5], ARGV[5], ARGV[6])
if redis.call('HGET', KEYS[3], 'mps_manifest') == '1' then
    redis.call('PERSIST', KEYS[4])
else
    redis.call('EXPIRE', KEYS[4], ARGV[10])
end
local sequence = redis.call('INCR', KEYS[9])
redis.call('ZADD', KEYS[8], tonumber(ARGV[11]) + sequence, ARGV[5])
redis.call('EXPIRE', KEYS[1], ARGV[12])
return 1
"""
