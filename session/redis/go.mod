module github.com/dobyte/due/session/redis/v2

go 1.25.0

require (
	github.com/dobyte/due/v2 v2.5.8
	github.com/redis/go-redis/v9 v9.17.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/sync v0.20.0 // indirect
)

replace github.com/dobyte/due/v2 => ../..
