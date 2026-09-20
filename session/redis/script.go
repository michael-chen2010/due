package redis

const acquireScript = `
local generation = redis.call('HINCRBY', KEYS[1], 'generation', 1)
redis.call('HSET', KEYS[1], 'current', generation)
return generation
`

const releaseScript = `
local current = redis.call('HGET', KEYS[1], 'current')
if not current or current ~= ARGV[1] then
	return 0
end
redis.call('HDEL', KEYS[1], 'current')
return 1
`
