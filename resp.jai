// jai-redis
// Smári McCarthy <smari@smarimccarthy.is>

RESPType :: enum u8 {
    NULL            :: 0;
    // RESP2
    ERROR           :: #char "-";
    INTEGER         :: #char ":";
    SIMPLE_STRING   :: #char "+";
    BULK_STRING     :: #char "$";
    ARRAY           :: #char "*";
    // RESP3
    BOOLEAN         :: #char "#";
    DOUBLE          :: #char ",";
    BIG_NUMBER      :: #char "(";
    VERBATIM        :: #char "=";
    MAP             :: #char "%";
    SET             :: #char "~";
    PUSH            :: #char ">";
}

// A parsed RESP reply. `value` used to be a union; it is now a plain struct so a
// message can carry e.g. a double's raw text *and* its parsed value at once, and
// so adding fields never aliases existing ones. The cost is a few extra bytes per
// message, which is negligible next to the heap strings each one owns.
RESPMessage :: struct {
    type  : RESPType;
    value : struct {
        s : string;          // SIMPLE_STRING, BULK_STRING, ERROR, VERBATIM, BIG_NUMBER, DOUBLE (raw text)
        i : s64;             // INTEGER, BOOLEAN (0/1)
        d : float64;         // DOUBLE (parsed; 0 for inf/nan, raw text is in `s`)
        a : [..]RESPMessage; // ARRAY, SET, PUSH, MAP (MAP is flat: key,value,key,value,...)
    };
}

RESP_Parse_Status :: enum {
    OK;
    INCOMPLETE; // not enough bytes buffered yet; read more and retry from the top
    ERROR;      // malformed wire data
}

// Recursively free everything a message owns. Safe on partially-built messages.
free_resp :: (msg: *RESPMessage) {
    if msg.type == {
        case .SIMPLE_STRING; #through;
        case .ERROR;         #through;
        case .BULK_STRING;   #through;
        case .VERBATIM;      #through;
        case .BIG_NUMBER;    #through;
        case .DOUBLE;
            if msg.value.s.data { free(msg.value.s.data); msg.value.s = ""; }
        case .ARRAY; #through;
        case .SET;   #through;
        case .PUSH;  #through;
        case .MAP;
            for * msg.value.a free_resp(it);
            array_free(msg.value.a);
            msg.value.a.data  = null;
            msg.value.a.count = 0;
    }
}

to_string :: (resp: *RESPMessage) -> string {
    if resp.type == {
        case .SIMPLE_STRING; return resp.value.s;
        case .BULK_STRING;   return resp.value.s;
        case .VERBATIM;      return resp.value.s;
        case .BIG_NUMBER;    return resp.value.s;
        case .ERROR;         return resp.value.s;
        case .INTEGER;       return tprint("%", resp.value.i);
        case .DOUBLE;        return ifx resp.value.s then resp.value.s else tprint("%", resp.value.d);
        case .BOOLEAN;       return ifx resp.value.i then "true" else "false";
        case .NULL;          return "(null)";
        case;                return tprint("<%>", resp.type);
    }
}

resp_is_null :: (msg: *RESPMessage) -> bool {
    return msg.type == .NULL;
}

resp_map_get :: (msg: *RESPMessage, key: string) -> *RESPMessage, bool {
    // Look up a value by key in a MAP reply (RESP3). Keys are matched as strings.
    if msg.type != .MAP return null, false;
    i := 0;
    while i + 1 < msg.value.a.count {
        k := *msg.value.a[i];
        if (k.type == .SIMPLE_STRING || k.type == .BULK_STRING) && k.value.s == key {
            return *msg.value.a[i + 1], true;
        }
        i += 2;
    }
    return null, false;
}

Resp_Cursor :: struct {
    data : *u8;
    len  : s64;
    pos  : s64;
}

resp_read_line :: (c: *Resp_Cursor) -> line: string, status: RESP_Parse_Status {
    // Read up to (not including) the next CRLF. Advances past the CRLF on success.
    i := c.pos;
    while i + 1 < c.len {
        if c.data[i] == #char "\r" && c.data[i + 1] == #char "\n" {
            line: string = ---;
            line.data  = c.data + c.pos;
            line.count = i - c.pos;
            c.pos = i + 2;
            return line, .OK;
        }
        i += 1;
    }
    return "", .INCOMPLETE;
}

resp_parse_len :: (s: string) -> value: s64, ok: bool {
    if s.count == 0 return 0, false;
    i   := 0;
    neg := false;
    if s[0] == #char "-" { neg = true; i = 1; }
    else if s[0] == #char "+" { i = 1; }
    if i >= s.count return 0, false;
    val: s64 = 0;
    while i < s.count {
        c := s[i];
        if c < #char "0" || c > #char "9" return 0, false;
        val = val * 10 + (c - #char "0");
        i += 1;
    }
    if neg val = -val;
    return val, true;
}

resp_copy :: (data: *u8, count: s64) -> string {
    view: string = .{ count, data };
    return copy_string(view);
}

resp_parse_bulk :: (c: *Resp_Cursor, out: *RESPMessage, payload: string, start: s64, type: RESPType) -> RESP_Parse_Status {
    n, ok := resp_parse_len(payload);
    if !ok { c.pos = start; return .ERROR; }
    if n < 0 { out.type = .NULL; return .OK; }          // $-1 / =-1  -> null
    if c.pos + n + 2 > c.len { c.pos = start; return .INCOMPLETE; }

    data  := c.data + c.pos;
    count := n;
    if type == .VERBATIM && count >= 4 {                // strip "txt:" / "mkd:" prefix
        data  += 4;
        count -= 4;
    }
    out.type    = type;
    out.value.s = resp_copy(data, count);
    c.pos += n + 2;                                     // data + trailing CRLF
    return .OK;
}

resp_parse_aggregate :: (c: *Resp_Cursor, out: *RESPMessage, payload: string, start: s64, type: RESPType) -> RESP_Parse_Status {
    n, ok := resp_parse_len(payload);
    if !ok { c.pos = start; return .ERROR; }
    if n < 0 { out.type = .NULL; return .OK; }          // *-1 -> null array

    out.type = type;
    total := ifx type == .MAP then n * 2 else n;        // maps are 2 elements per pair
    for 0..total - 1 {
        child: RESPMessage;
        st := resp_parse_item(c, *child);
        if st != .OK {
            free_resp(*child);
            return st;                                  // caller frees `out`
        }
        array_add(*out.value.a, child);
    }
    return .OK;
}

resp_parse_item :: (c: *Resp_Cursor, out: *RESPMessage) -> RESP_Parse_Status {
    start := c.pos;
    line, st := resp_read_line(c);
    if st != .OK { c.pos = start; return st; }
    if line.count == 0 { c.pos = start; return .ERROR; }

    payload: string = ---;
    payload.data  = line.data + 1;
    payload.count = line.count - 1;

    t := line[0];
    if t == {
        case #char "+";
            out.type = .SIMPLE_STRING;
            out.value.s = copy_string(payload);
            return .OK;
        case #char "-";
            out.type = .ERROR;
            out.value.s = copy_string(payload);
            return .OK;
        case #char ":";
            v, ok := resp_parse_len(payload);
            if !ok { c.pos = start; return .ERROR; }
            out.type = .INTEGER;
            out.value.i = v;
            return .OK;
        case #char "#";
            out.type = .BOOLEAN;
            out.value.i = ifx (payload.count > 0 && payload[0] == #char "t") then 1 else 0;
            return .OK;
        case #char ",";
            out.type = .DOUBLE;
            out.value.s = copy_string(payload);         // keep raw text (handles inf/nan)
            d, ok := string_to_float64(payload);
            if ok out.value.d = d;
            return .OK;
        case #char "(";
            out.type = .BIG_NUMBER;
            out.value.s = copy_string(payload);
            return .OK;
        case #char "_";
            out.type = .NULL;
            return .OK;
        case #char "$"; return resp_parse_bulk(c, out, payload, start, .BULK_STRING);
        case #char "="; return resp_parse_bulk(c, out, payload, start, .VERBATIM);
        case #char "*"; return resp_parse_aggregate(c, out, payload, start, .ARRAY);
        case #char "~"; return resp_parse_aggregate(c, out, payload, start, .SET);
        case #char ">"; return resp_parse_aggregate(c, out, payload, start, .PUSH);
        case #char "%"; return resp_parse_aggregate(c, out, payload, start, .MAP);
        case #char "|";
            // Attribute: a map of out-of-band metadata that precedes the real
            // reply. Parse and discard it, then parse the actual reply into `out`.
            attr: RESPMessage;
            ast := resp_parse_aggregate(c, *attr, payload, start, .MAP);
            free_resp(*attr);
            if ast != .OK return ast;
            return resp_parse_item(c, out);
        case;
            c.pos = start;
            return .ERROR;
    }
}

resp_parse_s64_or_zero :: (msg: RESPMessage) -> s64 {
    // Some stream sub-replies return counts as bulk strings (RESP2) or integers (RESP3); accept either.
    if msg.type == .INTEGER return msg.value.i;
    if msg.type == .BULK_STRING || msg.type == .SIMPLE_STRING {
        v, ok := resp_parse_len(msg.value.s);
        if ok return v;
    }
    return 0;
}


redis_read_reply :: (redis: *Redis) -> RESPMessage, bool {
    // Read exactly one complete RESP reply, growing/refilling the per-connection
    // buffer as needed. Returns (msg, ok). On failure ok=false and msg is empty.
    while true {
        c: Resp_Cursor;
        c.data = redis.recv_buf.data;
        c.len  = redis.recv_buf.count;
        c.pos  = 0;

        msg: RESPMessage;
        st := resp_parse_item(*c, *msg);

        if st == .OK {
            resp_consume_front(*redis.recv_buf, c.pos);
            return msg, true;
        }
        free_resp(*msg);
        if st == .ERROR {
            redis.recv_buf.count = 0;                   // desync: drop, don't loop forever
            empty: RESPMessage;
            return empty, false;
        }
        // INCOMPLETE: need more bytes.
        if !redis_recv_more(redis) {
            empty: RESPMessage;
            return empty, false;                        // socket closed or error
        }
    }
    empty: RESPMessage;
    return empty, false;
}

#scope_file
#import "Basic";
#import "String";
#import "Socket";
#if OS != .WINDOWS {
    #import "POSIX";
}

BUFFER_CHUNK :: 16384; // max bytes pulled per recv()

redis_recv_more :: (redis: *Redis) -> bool {
    // Append one socket read to the connection's buffer. Returns false on EOF/error.
    old := redis.recv_buf.count;
    array_resize(*redis.recv_buf, old + BUFFER_CHUNK);
    n := recv(redis.socket, redis.recv_buf.data + old, xx BUFFER_CHUNK, 0);
    if n <= 0 {
        array_resize(*redis.recv_buf, old);             // nothing read; undo growth
        return false;
    }
    array_resize(*redis.recv_buf, old + n);             // shrink to bytes actually read
    return true;
}

resp_consume_front :: (buf: *[..]u8, n: s64) {
    // Drop the first `n` bytes, keeping the remainder (and allocated capacity).
    if n <= 0 return;
    remaining := buf.count - n;
    if remaining > 0 memcpy(buf.data, buf.data + n, remaining);
    buf.count = max(remaining, 0);
}
