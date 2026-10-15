package redis

// Compare canonical decimal strings, never Lua floating-point integers. Redis
// HINCRBY remains exact up to signed MaxInt64; return HGET, not its Lua number.
const rangeStateScript = `
local max = "9223372036854775807"
local function valid(v)
 return v and string.match(v,"^%d+$") and (#v==1 or string.sub(v,1,1)~="0") and (#v<#max or (#v==#max and v<=max))
end
local function compare(a,b)
 if #a~=#b then if #a<#b then return -1 else return 1 end end
 if a==b then return 0 elseif a<b then return -1 else return 1 end
end
local function state()
 local generation=redis.call('HGET',KEYS[1],'generation')
 local current=redis.call('HGET',KEYS[1],'current')
 local limit=redis.call('HGET',KEYS[1],'range_end')
 if not generation then
  if current or limit then error("incomplete session ownership range metadata") end
  return "0",nil
 end
 if not valid(generation) or (current and (not valid(current) or current=="0" or compare(current,generation)>0)) or (limit and (not valid(limit) or compare(generation,limit)>0)) then
  error("invalid session ownership range metadata")
 end
 return generation,limit
end
local function advance()
 redis.call('HINCRBY',KEYS[1],'generation',1)
 local exact=redis.call('HGET',KEYS[1],'generation')
 redis.call('HSET',KEYS[1],'current',exact)
 return {"ok",exact}
end
`
const acquireScript = rangeStateScript + `
local generation,limit=state()
if limit and compare(generation,limit)<0 then return advance() end
return {"range",generation}
`
const initializeRangeScript = rangeStateScript + `
local generation,limit=state()
-- Another initializer can have won before this allocator acquired its lock.
-- Consume its valid range; the newly reserved unused range is just a gap.
if limit and compare(generation,limit)<0 then return advance() end
local first,last=ARGV[1],ARGV[2]
if not valid(first) or not valid(last) or first=="0" or compare(first,last)>0 or compare(first,generation)<=0 then
 return redis.error_reply("invalid session ownership reserved range")
end
redis.call('HSET',KEYS[1],'generation',first,'current',first,'range_end',last)
return {"ok",first}
`
const releaseScript = `
local current = redis.call('HGET', KEYS[1], 'current')
if not current or current ~= ARGV[1] then
 return 0
end
redis.call('HDEL', KEYS[1], 'current')
return 1
`
