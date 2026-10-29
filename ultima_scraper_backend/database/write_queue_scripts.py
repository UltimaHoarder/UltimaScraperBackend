"""Atomic, bounded Redis operations for the database writer transport.

Scripts operate on explicit keys and leave accepted commands intact on overload.
The notification list coalesces wakeups into one element; it carries no work.
"""

ENQUEUE = """
local result = redis.call('GET', KEYS[3])
if result then return {'completed', result} end
if redis.call('EXISTS', KEYS[2]) == 1 then return {'queued'} end
-- Bulk commands are routed by the partition layout the producer read. A stale
-- layout would split one record's mutations across two writer streams.
if ARGV[4] ~= '*' then
    local layout = redis.call('GET', KEYS[6]) or '1'
    if string.find(layout, '>', 1, true) then return {'switching', 0, 0} end
    if layout ~= ARGV[4] then return {'layout', layout} end
end
local count = redis.call('XLEN', KEYS[1])
local accounted = redis.call('GET', KEYS[4])
-- Before upgrading, drain the legacy stream. Never scan an unbounded backlog
-- in a script or pretend that its bytes have already been accounted for.
if not accounted and count > 0 then return {'legacy', 0, count} end
local bytes = tonumber(accounted or '0')
local size = string.len(ARGV[1])
if bytes + size > tonumber(ARGV[2]) or count >= tonumber(ARGV[3]) then
    return {'full', bytes, count}
end
redis.call('XADD', KEYS[1], '*', 'command', ARGV[1], 'bytes', size)
redis.call('INCRBY', KEYS[4], size)
-- A queued identity must survive an arbitrarily long outage. Completion
-- removes it atomically with result publication.
redis.call('SET', KEYS[2], 'queued')
redis.call('LPUSH', KEYS[5], '1')
redis.call('LTRIM', KEYS[5], 0, 0)
return {'queued'}
"""

ACKNOWLEDGE = """
local removed = 0
for i = 2, #ARGV do
    local entries = redis.call('XRANGE', KEYS[1], ARGV[i], ARGV[i], 'COUNT', 1)
    redis.call('XACK', KEYS[1], ARGV[1], ARGV[i])
    local deleted = redis.call('XDEL', KEYS[1], ARGV[i])
    if deleted == 1 and #entries == 1 then
        local fields = entries[1][2]
        for j = 1, #fields, 2 do
            if fields[j] == 'bytes' and redis.call('EXISTS', KEYS[2]) == 1 then
                redis.call('DECRBY', KEYS[2], fields[j + 1])
                break
            end
        end
        removed = removed + 1
    end
end
return removed
"""

READ_NEW = """
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
local cursor = nil
for _, group in ipairs(groups) do
    local name = nil
    local last = nil
    for i = 1, #group, 2 do
        if group[i] == 'name' then name = group[i + 1] end
        if group[i] == 'last-delivered-id' then last = group[i + 1] end
    end
    if name == ARGV[1] then cursor = last; break end
end
if not cursor then return redis.error_reply('NOGROUP database writer group missing') end
local count = 0
local bytes = 0
while count < tonumber(ARGV[3]) do
    local entries = redis.call('XRANGE', KEYS[1], '(' .. cursor, '+', 'COUNT', 1)
    if #entries == 0 then break end
    local size = 0
    for i = 1, #entries[1][2], 2 do
        size = size + string.len(entries[1][2][i + 1])
    end
    if bytes + size > tonumber(ARGV[4]) then
        if count == 0 and size > tonumber(ARGV[5] or ARGV[4]) then
            return redis.error_reply('Database write exceeds ULTIMA_DATABASE_WRITER_BATCH_MAX_BYTES; command remains queued')
        end
        break
    end
    bytes = bytes + size
    count = count + 1
    cursor = entries[1][1]
end
if count == 0 then return {} end
local result = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2],
    'COUNT', count, 'STREAMS', KEYS[1], '>')
if not result or #result == 0 then return {} end
return result[1][2]
"""

CLAIM_STALE = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1],
    '-', '+', ARGV[4])
local selected = {}
local bytes = 0
for _, entry in ipairs(pending) do
    -- A younger claim on the oldest entry is an ordering barrier. Filtering
    -- XPENDING by IDLE could skip it and replay a later mutation first.
    if tonumber(entry[3]) < tonumber(ARGV[3]) then break end
    -- A peer with a live heartbeat still owns its entries, however idle (a
    -- long transaction or a retry backoff). It is also an ordering barrier.
    if ARGV[6] and entry[2] ~= ARGV[2]
        and redis.call('EXISTS', ARGV[6] .. entry[2]) == 1 then break end
    local rows = redis.call('XRANGE', KEYS[1], entry[1], entry[1], 'COUNT', 1)
    if #rows > 0 then
        local size = 0
        for i = 1, #rows[1][2], 2 do
            size = size + string.len(rows[1][2][i + 1])
        end
        if bytes + size > tonumber(ARGV[5]) then
            if #selected == 0 then
                return redis.error_reply('Pending database write exceeds ULTIMA_DATABASE_WRITER_BATCH_MAX_BYTES; command remains queued')
            end
            break
        end
        bytes = bytes + size
        table.insert(selected, entry[1])
    else
        redis.call('XACK', KEYS[1], ARGV[1], entry[1])
    end
end
if #selected == 0 then return {} end
return redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], unpack(selected))
"""
