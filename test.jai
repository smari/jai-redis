#import "Basic";
#import,file "module.jai";

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
}


/*
    switch (try client.send(OrErr(i64), .{ "INCR", "stringkey" })) {
        .Ok, .Nil => unreachable,
        .Err => |err| std.debug.warn("error code = {}\n", .{err.getCode()}),
    }

    const MyHash = struct {
        banana: FixBuf(11),
        price: f32,
    };

    // Create a hash with the same fields as our struct
    try client.send(void, .{ "HSET", "myhash", "banana", "yes please", "price", "9.99" });

    // Parse it directly into the struct
    switch (try client.send(OrErr(MyHash), .{ "HGETALL", "myhash" })) {
        .Nil, .Err => unreachable,
        .Ok => |val| {
            std.debug.warn("myhash = \n\t{}\n", .{val});
        },
    }

    // Create a big string key
    try client.send(void, .{
        "SET",
        "divine",
        \\When half way through the journey of our life
        \\I found that I was in a gloomy wood,
        \\because the path which led aright was lost.
        \\And ah, how hard it is to say just what
        \\this wild and rough and stubborn woodland was,
        \\the very thought of which renews my fear!
    });

    // When you are fine with allocating memory,
    // you can use the .sendAlloc interface.
    const allocator = std.heap.page_allocator;

    // But then it's up to you to free all that was allocated.
    var inferno = try client.sendAlloc([]u8, allocator, .{ "GET", "divine" });
    defer allocator.free(inferno);
    std.debug.warn("\ndivine comedy - inferno 1: \n{}\n\n", .{inferno});

    // When using sendAlloc, you can use OrFullErr to parse not just the error code
    // but also the full error message. The error message is allocated with `allocator`
    // so it will need to be freed. (the next example will free it)
    const OrFullErr = okredis.types.OrFullErr;
    var incrErr = try client.sendAlloc(OrFullErr(i64), allocator, .{ "INCR", "divine" });
    switch (incrErr) {
        .Ok, .Nil => unreachable,
        .Err => |err| std.debug.warn("error code = {} message = '{}'\n", .{ err.getCode(), err.message }),
    }

    // To help deallocating resources allocated by `sendAlloc`, you can use `freeReply`.
    // `freeReply` knows how to deallocate values created by `sendAlloc`.
    const freeReply = okredis.freeReply;

    // For example, instead of freeing directly incrErr.Err.message, you can do this:
    defer freeReply(incrErr, allocator);

    // In general, sendAlloc will only allocate where the type you specify is a
    // pointer. This call doesn't require to free anything.
    _ = try client.sendAlloc(f64, allocator, .{ "HGET", "myhash", "price" });

    // This does require a free
    var allocatedNum = try client.sendAlloc(*f64, allocator, .{ "HGET", "myhash", "price" });
    defer freeReply(allocatedNum, allocator);
    // alternatively: defer allocator.destroy(allocatedNum);

    std.debug.warn("allocated num = {} ptr = {}\n", .{ allocatedNum.*, allocatedNum });

    // Now we can decode the reply in a struct that doesn't need a FixBuf
    const MyDynHash = struct {
        banana: []u8,
        price: f32,
    };

    const dynHash = try client.sendAlloc(OrErr(MyDynHash), allocator, .{ "HGETALL", "myhash" });
    defer freeReply(dynHash, allocator);

    switch (dynHash) {
        .Nil, .Err => unreachable,
        .Ok => |val| {
            std.debug.warn("mydynhash = \n\t{}\n", .{val});
        },
    }
    //   -
    //   == DYNAMIC REPLIES ==
    //   -

    // While most programs will use simple Redis commands, and will know
    // the shape of the reply, one might also be in a situation where the
    // reply is unknown or dynamic. To help with that, supredis includes
    // `DynamicReply`, which can decode any possible Redis reply.
    const DynamicReply = okredis.types.DynamicReply;
    var dynReply = try client.sendAlloc(DynamicReply, allocator, .{ "HGETALL", "myhash" });
    defer freeReply(dynReply, allocator);

    // DynamicReply is a union that represents all possible replies.
    std.debug.warn("\nmyhash decoded as DynamicReply:\n", .{});
    switch (dynReply.data) {
        .Nil, .Bool, .Number, .Double, .Bignum, .String, .List, .Set => {},
        .Map => |kvs| {
            for (kvs) |kv| {
                std.debug.warn("\t[{}] => '{}'\n", .{ kv[0].data.String.string, kv[1].data.String });
            }
        },
    }

    // Pipelining is a way of sending a batch of commands to Redis
    // in a more performant way than sending them one by one.
    // It's especially useful when using blocking I/O but, it can also
    // give small boosts when doing evented I/O.
    const r1 = try client.pipe(struct {
        c1: void,
        c2: u64,
        c3: OrErr(FixBuf(10)),
    }, .{
        .{ "SET", "counter", 0 },
        .{ "INCR", "counter" },
        .{ "ECHO", "banana" },
    });
    std.debug.warn("\n\n[INCR => {}]\n", .{r1.c2});
    std.debug.warn("[ECHO => {}]\n", .{r1.c3});

    // You can also allocate when doing pipelining.
    const r2 = try client.pipeAlloc(struct {
        c1: void,
        value: []u8,
    }, allocator, .{
        .{ "SET", "banana", "yes please" },
        .{ "GET", "banana" },
    });
    defer freeReply(r2, allocator);

    std.debug.warn("\n[banana] => '{}'\n", .{r2.value});

    // Transactions are a way of providing isolation and all-or-nothing semantics to
    // a group of Redis commands. The relative methods (`trans` and `transAlloc`) are
    // included mostly for convenience as they implicitly apply pipelining to the
    // commands passed, but the same result could be achieved by making explicit use
    // of MULTI/EXEC and `pipe`/`pipeAlloc`.
    switch (try client.trans(OrErr(struct {
        c1: OrErr(FixBuf(10)),
        c2: u64,
        c3: OrErr(void),
    }), .{
        .{ "SET", "banana", "no, thanks" },
        .{ "INCR", "counter" },
        .{ "INCR", "banana" },
    })) {
        .Err => |e| @panic(e.getCode()),
        .Nil => @panic("got nil"),
        .Ok => |tx_reply| {
            std.debug.warn("\n[SET = {}] [INCR = {}] [INCR (error) = {}]\n", .{
                tx_reply.c1.Ok.toSlice(),
                tx_reply.c2,
                tx_reply.c3.Err.getCode(),
            });
        },
    }
}
*/
