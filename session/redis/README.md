# Redis session ownership with durable generation ranges

`Store` implements the existing `(uid, uint64 generation)` fencing token. `Current` and `Release` retain the existing wire and Redis key formats. A hash now has `generation`, optional `current`, and `range_end`. No TTL is introduced.

For distributed allocation, pass `WithGenerationRangeAllocator(allocator)`. Its `WithGenerationRange(ctx, uid, floor, use)` implementation must durably reserve a never-reused inclusive range above both `floor` and every previously reserved range for the UID, commit before calling `use(operationCtx, start, end)`, and hold UID-exclusive execution ownership through that synchronous callback. An allocator error returns no Token even if initialization already happened. Unused reserved ranges are gaps and cannot be recycled.

Normal Acquire remains one Redis Lua operation while a reserved range has capacity. Missing, legacy, or exhausted metadata requires the durable allocator; without it, allocation fails closed with `ErrGenerationRangeRequired`. Read/control clients need no allocator. At signed MaxInt64, allocation returns `session.ErrOwnershipGenerationExhausted`.

Lua compares canonical decimal strings and returns HGET after HINCRBY, so generations above 2^53 remain exact. Initializers cannot overwrite a newer completed initialization; if another initializer has provided usable capacity, that existing range is consumed and the unused new reservation is skipped.

WebGameServer supplies a MySQL allocator with a connection-owned UID lock covering COMMIT and initialization. This module has no MySQL dependency. Test-only models in this module demonstrate behavior against real Redis; actual SQL durability and cross-instance acceptance belong to WebGameServer.

Before upgrading an active deployment, migrate the durable allocator schema and stop all legacy allocators/sessions. Legacy hashes provide an observable floor, but generations from a lost legacy hash cannot be reconstructed. Do not claim mixed-version compatibility, restore old hashes into an active session population, purge durable reservation rows, or treat these watermarks as ordinary expiring cache data. No deployment or data migration is performed by `NewStore`.
