#import "Basic";
#import,file "../module.jai";

pass := 0;
fail := 0;

check :: (cond: bool, desc: string) {
    if cond { pass += 1; print(" [PASS] %\n", desc); }
    else    { fail += 1; print(" [FAIL] %\n", desc); }
}

main :: () {
    redis := New(Redis);
    if !init(redis, "127.0.0.1", 6379) {
        print("Could not connect to Valkey at 127.0.0.1:6379\n");
        return;
    }
    defer deinit(redis);

    check(redis_enable_keepalive(redis, 15), "redis_enable_keepalive succeeds");

    S :: "test:stream";
    G :: "test:group";

    // Clean slate.
    { r := redis_send(redis, "DEL", S); free_resp(*r); }

    // --- XADD / XLEN / XRANGE ---
    id1, ok1 := redis_xadd(redis, S, "*", "temperature", "21.5", "humidity", "40");
    check(ok1 && id1.count > 0, "XADD returns an id");
    id2, ok2 := redis_xadd(redis, S, "*", "temperature", "22.0", "humidity", "41");
    check(ok2 && id2.count > 0, "XADD second entry");

    check(redis_xlen(redis, S) == 2, "XLEN is 2");

    {
        entries := redis_xrange(redis, S);
        defer free_stream_entries(*entries);
        check(entries.count == 2, "XRANGE returns 2 entries");
        if entries.count == 2 {
            t, found := entry_get(*entries[0], "temperature");
            check(found && t == "21.5", "first entry temperature field is 21.5");
            h, hfound := entry_get(*entries[1], "humidity");
            check(hfound && h == "41", "second entry humidity field is 41");
        }
    }

    // Consumer group lifecycle:
    check(redis_xgroup_create(redis, S, G, id="0"), "XGROUP CREATE at 0");

    {
        reads, ok := redis_xreadgroup(redis, G, "consumer-A", .[S], .[">"], count=10);
        defer free_stream_reads(*reads);
        check(ok && reads.count == 1, "XREADGROUP returns one stream");
        if reads.count == 1 {
            check(reads[0].entries.count == 2, "XREADGROUP delivers 2 entries to consumer-A");
        }
    }

    {
        p := redis_xpending(redis, S, G);
        defer free_pending_summary(*p);
        check(p.count == 2, "XPENDING shows 2 unacked");
    }

    // Ack the first entry; one should remain pending.
    check(redis_xack(redis, S, G, id1) == 1, "XACK acknowledges 1 message");
    {
        p := redis_xpending(redis, S, G);
        defer free_pending_summary(*p);
        check(p.count == 1, "XPENDING shows 1 unacked after XACK");
    }

    // Dead-consumer recovery via XAUTOCLAIM:
    // id2 is still pending for consumer-A. consumer-B claims anything idle >= 0ms.
    {
        ac := redis_xautoclaim(redis, S, G, "consumer-B", 0, start="0");
        defer free_auto_claim(*ac);
        check(ac.entries.count == 1, "XAUTOCLAIM moves the 1 pending entry to consumer-B");
        if ac.entries.count == 1 {
            check(ac.entries[0].id == id2, "claimed entry is id2");
        }
    }
    {
        // Now id2 is owned by consumer-B; detailed XPENDING should attribute it.
        pes := redis_xpending_range(redis, S, G, count=10);
        defer free_pending_entries(*pes);
        check(pes.count == 1, "XPENDING range shows 1 entry");
        if pes.count == 1 check(pes[0].consumer == "consumer-B", "pending entry now owned by consumer-B");
    }

    // MAXLEN trimming
    {
        T :: "test:capped";
        { r := redis_send(redis, "DEL", T); free_resp(*r); }
        for 1..10 {
            id, ok := redis_xadd_maxlen(redis, T, 5, false, "*", "n", tprint("%", it));
        }
        check(redis_xlen(redis, T) == 5, "XADD MAXLEN 5 caps the stream at 5");
        { r := redis_send(redis, "DEL", T); free_resp(*r); }
    }

    // XINFO
    {
        info, ok := redis_xinfo_stream(redis, S);
        defer free_resp(*info);
        check(ok && info.type == .MAP, "XINFO STREAM returns a map");
        len_field, found := resp_map_get(*info, "length");
        check(found, "XINFO STREAM has a 'length' field");
    }

    // Cleanup.
    { r := redis_send(redis, "DEL", S); free_resp(*r); }

    print("\n%/% checks passed.\n", pass, pass + fail);
}
