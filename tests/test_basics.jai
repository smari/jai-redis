#import "Basic";
#import,file "../module.jai";

is_equal :: (x: string, y: string, desc: string) {
    if (x == y) print(" [PASS] %\n", desc);
    else        print(" [FAIL] %    (x='%', y='%')\n", desc, x, y);
}

is_equal_bool :: (x: bool, y: bool, desc: string) {
    if (x == y) print(" [PASS] %\n", desc);
    else        print(" [FAIL] %\n", desc);
}

is_equal_int :: (x: s64, y: s64, desc: string) {
    if (x == y) print(" [PASS] %\n", desc);
    else        print(" [FAIL] %\n", desc);
}


main :: () {
    redis := New(Redis);
    init(redis, "127.0.0.1", 6379);
    defer deinit(redis);

    // Try setting a value manually and getting it again
    {
        reply := redis_send(redis, "SET", "key", "value");
        is_equal(reply.value.s, "OK", "Manual SET succeeds");
        reply2 := redis_send(redis, "GET", "key");
        is_equal(reply2.value.s, "value", "Manual GET succeeds");
    }

    // Test error handling:
    {
        reply, success := redis_send(redis, "GET", "nokey");
        is_equal(reply.value.s, "", "Test get on unset key returns Null");
    }

    // And now for some utility functions:
    {
        res := redis_set(redis, "ltuae", 42);
        is_equal_bool(res, true, "Test SET succeeds");
        r2  := redis_get(redis, "ltuae");
        is_equal(r2, "42", "Test GET returns correct value");
    }

    {
        res1 := redis_getset(redis, "key", "bloop");
        is_equal(res1, "value", "Test GETSET returned previous value");
        res2 := redis_get(redis, "key");
        is_equal(res2, "bloop", "Test GETSET succeeded in setting");
    }

    {
        suc, res := redis_set(redis, "exkey", "foo", ex=5);
        is_equal_bool(suc, true, "Test SET with EX=5 succeeds");
    }

    {
        len := redis_strlen(redis, "exkey");
        is_equal_int(xx len, 3, "Test STRLEN");
    }

    {
        redis_append(redis, "key", "bla");
        res := redis_get(redis, "key");
        is_equal(res, "bloopbla", "Test APPEND returns expected value");
    }

    // Value far larger than the old 4096 ceiling and the 16384 recv chunk, so it
    // must span several reads and exercise the growable buffer + read loop.
    {
        N :: 200000;
        data := alloc(N);
        memset(data, #char "x", N);
        big: string;
        big.data  = data;
        big.count = N;

        ok := redis_set(redis, "bigkey", big);
        is_equal_bool(ok, true, "Large SET (200KB) succeeds");
        got := redis_get(redis, "bigkey");
        is_equal_int(got.count, N, "Large value round-trips with correct length");
    }

    // Argument encoding for non-string/non-s64 types (the old code dropped these).
    {
        redis_send(redis, "SET", "i64max", 9223372036854775807);
        v := redis_get(redis, "i64max");
        is_equal(v, "9223372036854775807", "s64 max encodes as argument");

        redis_send(redis, "SET", "boolarg", true);
        b := redis_get(redis, "boolarg");
        is_equal(b, "1", "bool arg encodes as '1'");
    }

    // RESP3 map: with HELLO 3 negotiated, HGETALL returns a map (%), not a flat array.
    {
        redis_send(redis, "DEL", "h");
        redis_send(redis, "HSET", "h", "a", "1", "b", "2");
        m, ok := redis_send(redis, "HGETALL", "h");
        is_equal_bool(m.type == .MAP, true, "HGETALL returns a RESP3 map");
        av, found := resp_map_get(*m, "a");
        is_equal_bool(found, true, "map key 'a' present");
        if found is_equal(av.value.s, "1", "map value for 'a' is '1'");
        free_resp(*m);
    }
}
