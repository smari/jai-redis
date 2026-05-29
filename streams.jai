// Redis Streams + consumer groups.
//
// Typed wrappers over the X* command family. Reply-parsing helpers copy the
// relevant bytes out of the raw RESPMessage into the structs below, so callers
// own the returned structs and free them with the free_* helpers here; the raw
// RESPMessage is freed internally.
//
// Note on blocking reads: redis_xread/redis_xreadgroup take block_ms. A value of
// 0 blocks forever, which ties up the (single, non-thread-safe) connection until
// data arrives. Prefer a finite timeout unless you really want to park the
// connection. -1 is non-blocking, which is default.

Stream_Field :: struct {
    name:  string;
    value: string;
}

Stream_Entry :: struct {
    id:     string;
    fields: [..] Stream_Field;
}

// One stream's worth of entries, as returned by XREAD / XREADGROUP.
Stream_Read :: struct {
    stream:  string;
    entries: [..] Stream_Entry;
}

// Look up a field value within an entry by name.
entry_get :: (entry: *Stream_Entry, name: string) -> string, bool {
    for * entry.fields {
        if it.name == name return it.value, true;
    }
    return "", false;
}

parse_stream_entry :: (msg: *RESPMessage) -> Stream_Entry {
    // An entry is [ id, [field, value, field, value, ...] ]. The field list may be
    // null (e.g. an entry that was deleted but still sits in a PEL).
    e: Stream_Entry;
    if msg.type != .ARRAY || msg.value.a.count < 2 return e;

    e.id = copy_string(msg.value.a[0].value.s);

    fields := *msg.value.a[1];
    if fields.type == .ARRAY {
        i := 0;
        while i + 1 < fields.value.a.count {
            f: Stream_Field;
            f.name  = copy_string(fields.value.a[i].value.s);
            f.value = copy_string(fields.value.a[i + 1].value.s);
            array_add(*e.fields, f);
            i += 2;
        }
    }
    return e;
}

parse_stream_entries :: (msg: *RESPMessage) -> [..] Stream_Entry {
    // An array of entries (XRANGE / XCLAIM / the per-stream payload of XREAD).
    out: [..] Stream_Entry;
    if msg.type != .ARRAY return out;
    for * msg.value.a {
        e := parse_stream_entry(it);
        array_add(*out, e);
    }
    return out;
}

parse_stream_reads :: (msg: *RESPMessage) -> [..] Stream_Read {
    // XREAD/XREADGROUP reply: a RESP3 map { stream -> entries } or, under RESP2, an
    // array of [ stream, entries ] pairs.
    out: [..] Stream_Read;
    if msg.type == .MAP {
        i := 0;
        while i + 1 < msg.value.a.count {
            r: Stream_Read;
            r.stream  = copy_string(msg.value.a[i].value.s);
            r.entries = parse_stream_entries(*msg.value.a[i + 1]);
            array_add(*out, r);
            i += 2;
        }
    } else if msg.type == .ARRAY {
        for * msg.value.a {
            if it.type == .ARRAY && it.value.a.count >= 2 {
                r: Stream_Read;
                r.stream  = copy_string(it.value.a[0].value.s);
                r.entries = parse_stream_entries(*it.value.a[1]);
                array_add(*out, r);
            }
        }
    }
    return out;
}

redis_xadd :: (redis: *Redis, stream: string, id: string, fields: .. Any) -> string, bool {
    // XADD stream id field value [field value ...]. Pass id="*" for a server-assigned
    // id. `fields` is a flat list: name, value, name, value, ...
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XADD");
    array_add(*args, stream);
    array_add(*args, id);
    for fields array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    if !ok || ret.type != .BULK_STRING return "", false;
    return copy_string(ret.value.s), true;
}

redis_xadd_maxlen :: (redis: *Redis, stream: string, maxlen: s64, approx: bool, id: string, fields: .. Any) -> string, bool {
    // XADD with a capped length: XADD stream MAXLEN [~] <maxlen> id field value ...
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XADD");
    array_add(*args, stream);
    array_add(*args, "MAXLEN");
    if approx array_add(*args, "~");
    array_add(*args, maxlen);
    array_add(*args, id);
    for fields array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    if !ok || ret.type != .BULK_STRING return "", false;
    return copy_string(ret.value.s), true;
}

redis_xlen :: (redis: *Redis, stream: string) -> s64 {
    ret, ok := redis_send(redis, "XLEN", stream);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xdel :: (redis: *Redis, stream: string, ids: .. Any) -> s64 {
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XDEL");
    array_add(*args, stream);
    for ids array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xtrim_maxlen :: (redis: *Redis, stream: string, maxlen: s64, approx := false) -> s64 {
    ret: RESPMessage;
    if approx ret, _ = redis_send(redis, "XTRIM", stream, "MAXLEN", "~", maxlen);
    else      ret, _ = redis_send(redis, "XTRIM", stream, "MAXLEN", maxlen);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xtrim_minid :: (redis: *Redis, stream: string, minid: string, approx := false) -> s64 {
    ret: RESPMessage;
    if approx ret, _ = redis_send(redis, "XTRIM", stream, "MINID", "~", minid);
    else      ret, _ = redis_send(redis, "XTRIM", stream, "MINID", minid);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xsetid :: (redis: *Redis, stream: string, id: string) -> bool {
    ret, ok := redis_send(redis, "XSETID", stream, id);
    defer free_resp(*ret);
    return ok && ret.type == .SIMPLE_STRING;
}

redis_xrange :: (redis: *Redis, stream: string, start := "-", end := "+", count: s64 = -1) -> [..] Stream_Entry {
    // XRANGE stream start end [COUNT count]. Defaults span the whole stream.
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XRANGE");
    array_add(*args, stream);
    array_add(*args, start);
    array_add(*args, end);
    if count >= 0 { array_add(*args, "COUNT"); array_add(*args, count); }

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    return parse_stream_entries(*ret);
}

redis_xrevrange :: (redis: *Redis, stream: string, end := "+", start := "-", count: s64 = -1) -> [..] Stream_Entry {
    // XREVRANGE stream end start [COUNT count] (note the reversed bound order).
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XREVRANGE", stream, end, start);
    if count >= 0 { array_add(*args, "COUNT"); array_add(*args, count); }

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    return parse_stream_entries(*ret);
}

redis_xread :: (redis: *Redis, streams: [] string, ids: [] string, count: s64 = -1, block_ms: s64 = -1) -> [..] Stream_Read, bool {
    // XREAD [COUNT n] [BLOCK ms] STREAMS s1 s2 ... id1 id2 ...
    // `streams` and `ids` must be the same length. ok is false on null/timeout.
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XREAD");
    if count    >= 0 { array_add(*args, "COUNT"); array_add(*args, count); }
    if block_ms >= 0 { array_add(*args, "BLOCK"); array_add(*args, block_ms); }
    array_add(*args, "STREAMS");
    for streams array_add(*args, it);
    for ids     array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    if !ok || ret.type == .NULL {
        empty: [..] Stream_Read;
        return empty, false;
    }
    return parse_stream_reads(*ret), true;
}

redis_xreadgroup :: (redis: *Redis, group: string, consumer: string, streams: [] string, ids: [] string, count: s64 = -1, block_ms: s64 = -1, noack := false) -> [..] Stream_Read, bool {
    // XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS s1 ... id1 ...
    // Use id ">" to fetch never-delivered messages, or an explicit id to re-read PEL.
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XREADGROUP", "GROUP", group, consumer);
    if count    >= 0 { array_add(*args, "COUNT"); array_add(*args, count); }
    if block_ms >= 0 { array_add(*args, "BLOCK"); array_add(*args, block_ms); }
    if noack         array_add(*args, "NOACK");
    array_add(*args, "STREAMS");
    for streams array_add(*args, it);
    for ids     array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    if !ok || ret.type == .NULL {
        empty: [..] Stream_Read;
        return empty, false;
    }
    return parse_stream_reads(*ret), true;
}

redis_xgroup_create :: (redis: *Redis, stream: string, group: string, id := "$", mkstream := false) -> bool {
    // XGROUP CREATE stream group id [MKSTREAM]. id "$" = only new messages, "0" = all.
    ret: RESPMessage;
    ok:  bool;
    if mkstream ret, ok = redis_send(redis, "XGROUP", "CREATE", stream, group, id, "MKSTREAM");
    else        ret, ok = redis_send(redis, "XGROUP", "CREATE", stream, group, id);
    defer free_resp(*ret);
    return ok && ret.type == .SIMPLE_STRING;
}

redis_xgroup_setid :: (redis: *Redis, stream: string, group: string, id: string) -> bool {
    ret, ok := redis_send(redis, "XGROUP", "SETID", stream, group, id);
    defer free_resp(*ret);
    return ok && ret.type == .SIMPLE_STRING;
}

redis_xgroup_destroy :: (redis: *Redis, stream: string, group: string) -> s64 {
    ret, ok := redis_send(redis, "XGROUP", "DESTROY", stream, group);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xgroup_createconsumer :: (redis: *Redis, stream: string, group: string, consumer: string) -> s64 {
    ret, ok := redis_send(redis, "XGROUP", "CREATECONSUMER", stream, group, consumer);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xgroup_delconsumer :: (redis: *Redis, stream: string, group: string, consumer: string) -> s64 {
    ret, ok := redis_send(redis, "XGROUP", "DELCONSUMER", stream, group, consumer);
    defer free_resp(*ret);
    return ret.value.i;
}

redis_xack :: (redis: *Redis, stream: string, group: string, ids: .. Any) -> s64 {
    // XACK stream group id [id ...] -> number of messages acknowledged.
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XACK", stream, group);
    for ids array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    return ret.value.i;
}

Pending_Consumer :: struct {
    name:  string;
    count: s64;
}

Pending_Summary :: struct {
    count:     s64;
    min_id:    string;
    max_id:    string;
    consumers: [..] Pending_Consumer;
}

Pending_Entry :: struct {
    id:             string;
    consumer:       string;
    idle_ms:        s64;
    delivery_count: s64;
}

redis_xpending :: (redis: *Redis, stream: string, group: string) -> Pending_Summary {
    // XPENDING stream group -> summary: [ count, min-id, max-id, [[consumer,count]...] ].
    s: Pending_Summary;
    ret, ok := redis_send(redis, "XPENDING", stream, group);
    defer free_resp(*ret);
    if ret.type != .ARRAY || ret.value.a.count < 4 return s;

    s.count  = ret.value.a[0].value.i;
    s.min_id = copy_string(ret.value.a[1].value.s);
    s.max_id = copy_string(ret.value.a[2].value.s);

    consumers := *ret.value.a[3];
    if consumers.type == .ARRAY {
        for * consumers.value.a {
            if it.type == .ARRAY && it.value.a.count >= 2 {
                pc: Pending_Consumer;
                pc.name  = copy_string(it.value.a[0].value.s);
                // count comes back as a bulk string in this sub-reply
                pc.count = resp_parse_s64_or_zero(it.value.a[1]);
                array_add(*s.consumers, pc);
            }
        }
    }
    return s;
}

redis_xpending_range :: (redis: *Redis, stream: string, group: string, start := "-", end := "+", count: s64 = 10, consumer := "", min_idle_ms: s64 = -1) -> [..] Pending_Entry {
    // XPENDING stream group [IDLE ms] start end count [consumer] -> detailed entries:
    // each [ id, consumer, idle-ms, delivery-count ].
    out: [..] Pending_Entry;
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XPENDING", stream, group);
    if min_idle_ms >= 0 { array_add(*args, "IDLE"); array_add(*args, min_idle_ms); }
    array_add(*args, start, end, count);
    if consumer.count > 0 array_add(*args, consumer);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    if ret.type != .ARRAY return out;

    for * ret.value.a {
        if it.type == .ARRAY && it.value.a.count >= 4 {
            pe: Pending_Entry;
            pe.id             = copy_string(it.value.a[0].value.s);
            pe.consumer       = copy_string(it.value.a[1].value.s);
            pe.idle_ms        = it.value.a[2].value.i;
            pe.delivery_count = resp_parse_s64_or_zero(it.value.a[3]);
            array_add(*out, pe);
        }
    }
    return out;
}

redis_xclaim :: (redis: *Redis, stream: string, group: string, consumer: string, min_idle_ms: s64, ids: .. Any) -> [..] Stream_Entry {
    // XCLAIM stream group consumer min-idle-ms id [id ...] -> claimed entries.
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XCLAIM", stream, group, consumer, min_idle_ms);
    for ids array_add(*args, it);

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    return parse_stream_entries(*ret);
}

Auto_Claim :: struct {
    cursor:  string;              // pass back as `start` to continue scanning
    entries: [..] Stream_Entry;
    deleted: [..] string;         // ids that no longer exist (dropped from the PEL)
}

redis_xautoclaim :: (redis: *Redis, stream: string, group: string, consumer: string, min_idle_ms: s64, start := "0", count: s64 = -1) -> Auto_Claim {
    // XAUTOCLAIM stream group consumer min-idle-ms start [COUNT n] -> [ cursor, [entries], [deleted-ids] ].
    ac: Auto_Claim;
    args: [..] Any;
    defer array_free(args);
    array_add(*args, "XAUTOCLAIM", stream, group, consumer, min_idle_ms, start);
    if count >= 0 { array_add(*args, "COUNT"); array_add(*args, count); }

    ret, ok := redis_send(redis, ..args);
    defer free_resp(*ret);
    if ret.type != .ARRAY || ret.value.a.count < 2 return ac;

    ac.cursor  = copy_string(ret.value.a[0].value.s);
    ac.entries = parse_stream_entries(*ret.value.a[1]);
    if ret.value.a.count >= 3 {
        deleted := *ret.value.a[2];
        if deleted.type == .ARRAY {
            for * deleted.value.a array_add(*ac.deleted, copy_string(it.value.s));
        }
    }
    return ac;
}

redis_xinfo_stream :: (redis: *Redis, stream: string) -> RESPMessage, bool {
    msg, ok := redis_send(redis, "XINFO", "STREAM", stream);
    return msg, ok;
}

redis_xinfo_groups :: (redis: *Redis, stream: string) -> RESPMessage, bool {
    msg, ok := redis_send(redis, "XINFO", "GROUPS", stream);
    return msg, ok;
}

redis_xinfo_consumers :: (redis: *Redis, stream: string, group: string) -> RESPMessage, bool {
    msg, ok := redis_send(redis, "XINFO", "CONSUMERS", stream, group);
    return msg, ok;
}

free_stream_entry :: (e: *Stream_Entry) {
    if e.id.data free(e.id.data);
    for * e.fields {
        if it.name.data  free(it.name.data);
        if it.value.data free(it.value.data);
    }
    array_free(e.fields);
    e.fields.data  = null;
    e.fields.count = 0;
}

free_stream_entries :: (entries: *[..] Stream_Entry) {
    for * entries.* free_stream_entry(it);
    array_free(entries.*);
    entries.data  = null;
    entries.count = 0;
}

free_stream_reads :: (reads: *[..] Stream_Read) {
    for * reads.* {
        if it.stream.data free(it.stream.data);
        free_stream_entries(*it.entries);
    }
    array_free(reads.*);
    reads.data  = null;
    reads.count = 0;
}

free_pending_summary :: (s: *Pending_Summary) {
    if s.min_id.data free(s.min_id.data);
    if s.max_id.data free(s.max_id.data);
    for * s.consumers if it.name.data free(it.name.data);
    array_free(s.consumers);
    s.consumers.data  = null;
    s.consumers.count = 0;
}

free_pending_entries :: (entries: *[..] Pending_Entry) {
    for * entries.* {
        if it.id.data       free(it.id.data);
        if it.consumer.data free(it.consumer.data);
    }
    array_free(entries.*);
    entries.data  = null;
    entries.count = 0;
}

free_auto_claim :: (ac: *Auto_Claim) {
    if ac.cursor.data free(ac.cursor.data);
    free_stream_entries(*ac.entries);
    for * ac.deleted if it.data free(it.data);
    array_free(ac.deleted);
    ac.deleted.data  = null;
    ac.deleted.count = 0;
}
