# Valkey/Redis API for Jai

A Valkey/Redis client for Jai.

See `tests` for usage examples and tests.

## Features

 * RESP3 protocol (`HELLO 3`), with RESP2 replies still parsed.
 * Basics: `GET`, `SET`, `LPUSH`, `LPOP`, `BLPOP`, `INFO`, `GETRANGE`, `SETRANGE`, 
   `APPEND`, `STRLEN`...
 * Strings, lists, hashes, and the full Redis Streams / consumer-group command set
   (`XADD`, `XRANGE`, `XREAD`, `XREADGROUP`, `XACK`, `XGROUP`, `XPENDING`, `XCLAIM`,
   `XAUTOCLAIM`, `XINFO`, ...).
 * TCP keep-alive (`redis_enable_keepalive`), mirroring hiredis `redisEnableKeepAlive`.

## TODO

 * Reconnect-on-failure and socket read/write timeouts.
 * Explicit pipelining (append command / get reply).
 * Unix domain socket connections.
 * Broader command coverage (sets, sorted sets, pub/sub via RESP3 push).
 ... and more stuff probably.
