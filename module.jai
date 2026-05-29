// jai-redis
// Redis bindings for Jai
// Smári McCarthy <smari@smarimccarthy.is>

#load "resp.jai";
#load "streams.jai";

// A Redis instance is single-connection and not thread-safe; use one per thread (or add your own locking). 
Redis :: struct {
    socket:     Socket;
    host:       string;
    port:       u16;
    flags:      RedisFlags;
    recv_buf:   [..]u8; // Holds bytes already pulled off the socket but not yet consumed by a reply.
}

RedisFlags :: enum_flags u16 {
    RESP3;      // Whether the server is known to support RESP3 or not. May change what we send.
}

init :: (redis: *Redis, host: string, port: u16, user: string = "default", password: string = "") -> bool {
    socket_init();

    redis.host = host;
    redis.port = port;
    redis.socket = socket(AF_INET, .SOCK_STREAM, .TCP);
    if redis.socket < 0 {
        log("Failed to create socket for redis.\n");
        return false;
    }

    server_address : sockaddr_in;

    // assign IP, PORT
    server_address.sin_family = AF_INET;
    server_address.sin_port   = htons(port);
    #if OS == .WINDOWS {
        server_address.sin_addr.S_un.S_addr = inet_addr(to_c_string(host));
    } else {
    	server_address.sin_addr.s_addr = inet_addr(to_c_string(host));
    }

    // connect the client socket to server socket
    if (connect(redis.socket, xx *server_address, xx size_of(sockaddr_in)) != 0) {
        log("Failed to connect to redis server (%:%)\n", host, port);
        return false;
    }

    // Negotiate RESP3 if available.
    resp : RESPMessage;
    ok   : bool;
    if password.count > 0 {
        resp, ok = redis_send(redis, "HELLO", 3, "AUTH", user, password);
    } else {
        resp, ok = redis_send(redis, "HELLO", 3);
    }
    defer free_resp(*resp);

    if !ok {
        log("Redis handshake failed (%:%) - no reply!\n", host, port, flags=.ERROR);
        return false;
    }
    if resp.type == .ERROR {
        log("Redis handshake failed (%:%) - '%'!\n", host, port, resp.value.s, flags=.ERROR);
        return false;
    }

    // The HELLO reply is a map advertising the negotiated protocol in `proto`.
    // If it reports >= 3 the server switched us to RESP3; record that.
    proto, found := resp_map_get(*resp, "proto");
    if found && resp_parse_s64_or_zero(proto.*) >= 3 {
        redis.flags |= .RESP3;
    }

    return true;
}

deinit :: (redis: *Redis) {
    close_and_reset(*redis.socket);
    array_free(redis.recv_buf);
    redis.recv_buf.data  = null;
    redis.recv_buf.count = 0;
}

redis_enable_keepalive :: (redis: *Redis, interval_seconds: s32 = 15) -> bool {
    // Enable TCP keep-alive on the connection. Same as hiredis' redisEnableKeepAlive.
    probe_interval := interval_seconds / 3;
    if probe_interval < 1 probe_interval = 1;
    return set_keepalive(redis.socket, true, interval_seconds, probe_interval, 3);
}

redis_select_db :: (redis: *Redis, db: int) -> bool {
    ret, suc := redis_send(redis, "SELECT", db);
    defer free_resp(*ret);
    return suc && ret.type != .ERROR;
}

redis_send :: (redis: *Redis, args: .. Any) -> RESPMessage, bool {
    // Send a command and read one reply. `ok` means a well-formed reply was received
    // (an ERROR-type reply still returns ok=true; inspect ret.type). The caller owns
    // the returned RESPMessage and should free_resp() it when done.
    sb: String_Builder;
    init_string_builder(*sb);
    print_to_builder(*sb, "*%\r\n", args.count);
    for arg: args {
        s := any_to_redis_string(arg);
        print_to_builder(*sb, "$%\r\n", s.count);
        append(*sb, s);
        append(*sb, "\r\n");
    }
    req := builder_to_string(*sb);
    defer free(req);

    if redis_send_all(redis.socket, req) < 0 {
        empty: RESPMessage;
        return empty, false;
    }
    msg, ok := redis_read_reply(redis);
    return msg, ok;
}

redis_pop :: (redis: *Redis, list: string, blocking: bool=false) -> string, bool {
    ret, suc := redis_send(redis, ifx blocking then "BLPOP" else "LPOP", list, 60);
    defer free_resp(*ret);

    if ret.type == .ERROR return ret.value.s, false;
    if ret.type == .NULL return "", false;

    if ret.type == .ARRAY {
        if ret.value.a.count > 1 {
            // index 0 is the name of the list we retrieved from...
            return copy_string(ret.value.a[1].value.s), true;
        }
    }

    return "", false;
}

redis_push :: (redis: *Redis, list: string, value: Any) -> string, bool {
    ret, suc := redis_send(redis, "LPUSH", list, value);
    defer free_resp(*ret);
    if ret.type == .ERROR return ret.value.s, false;
    if ret.type == .NULL return "", false;
    return to_string(*ret), true;
}

redis_info :: (redis: *Redis) -> string {
    ret, suc := redis_send(redis, "INFO");
    defer free_resp(*ret);
    return copy_string(ret.value.s);
}

redis_set :: (redis: *Redis, key: string, value: Any, ex:=0, px:=0, nx:=false, _xx:=false, getset:=false) -> bool, RESPMessage {
    /* Options:
     * [EX seconds|PX milliseconds|KEEPTTL] [NX|XX] [GET]
     * See https://redis.io/commands/set
     */
    ret: RESPMessage;
    suc: bool;
    opts: [..]Any;
    if getset array_add(*opts, "GETSET");
    else      array_add(*opts, "SET");
    array_add(*opts, key);
    array_add(*opts, value);
    if ex {
        array_add(*opts, "EX");
        array_add(*opts, ex);
    } else if px {
        array_add(*opts, "PX");
        array_add(*opts, ex);
    }
    if nx       array_add(*opts, "NX");
    else if _xx array_add(*opts, "XX");

    ret, suc = redis_send(redis, ..opts);

    if ret.type == .SIMPLE_STRING && ret.value.s == "OK" return true, ret;
    return false, ret;
}


redis_get :: (redis: *Redis, key: string) -> string, bool {
    ret, suc := redis_send(redis, "GET", key);
    defer free_resp(*ret);
    if ret.type == .ERROR return copy_string(ret.value.s), false;
    if ret.type == .NULL return "", false;
    return copy_string(ret.value.s), true;
}


redis_getset :: (redis: *Redis, key: string, value: Any, ex:=0, px:=0, nx:=false, _xx:=false) -> string, bool {
    suc, ret := redis_set(redis, key, value, ex, px, nx, _xx, getset=true);
    defer free_resp(*ret);
    if ret.type == .ERROR return copy_string(ret.value.s), false;
    if ret.type == .NULL return "", false;
    return copy_string(ret.value.s), true;
}


redis_strlen :: (redis: *Redis, key: string) -> s64 {
    ret, suc := redis_send(redis, "STRLEN", key);
    defer free_resp(*ret);
    return ret.value.i;
}


redis_append :: (redis: *Redis, key: string, append: string) -> bool {
    ret, suc := redis_send(redis, "APPEND", key, append);
    defer free_resp(*ret);
    return suc && ret.type != .ERROR;
}


redis_getrange :: (redis: *Redis, key: string, start: int, end: int) -> string, bool {
    ret, suc := redis_send(redis, "GETRANGE", key, start, end);
    defer free_resp(*ret);
    return copy_string(ret.value.s), suc;
}


redis_setrange :: (redis: *Redis, key: string, start: int, value: Any) -> bool {
    ret, suc := redis_send(redis, "SETRANGE", key, start, value);
    defer free_resp(*ret);
    return suc && ret.type != .ERROR;
}



#scope_file
#import "Socket";

redis_send_all :: (sock: Socket, s: string) -> s64 {
    // Write the whole request, looping over partial sends.
    total: s64 = 0;
    while total < s.count {
        n := send(sock, s.data + total, xx (s.count - total), 0);
        if n <= 0 return -1;
        total += n;
    }
    return total;
}

any_to_redis_string :: (arg: Any) -> string {
    // Render an argument to the text Redis expects. Every arg is sent as a bulk
    // string; this just produces the bytes. Handles all integer widths, floats,
    // bools and strings (the old code silently dropped everything but s64/string,
    // which corrupted the argument count).
    info := arg.type;
    if info.type == {
        case .STRING;
            return (cast(*string) arg.value_pointer).*;
        case .BOOL;
            return ifx (cast(*bool) arg.value_pointer).* then "1" else "0";
        case .FLOAT;
            if info.runtime_size == 4 return tprint("%", (cast(*float32) arg.value_pointer).*);
            return tprint("%", (cast(*float64) arg.value_pointer).*);
        case .INTEGER;
            ii := cast(*Type_Info_Integer) info;
            return read_int_any(arg.value_pointer, info.runtime_size, ii.signed);
        case .ENUM;
            ei := cast(*Type_Info_Enum) info;
            return read_int_any(arg.value_pointer, info.runtime_size, ei.internal_type.signed);
        case;
            return tprint("%", arg);
    }
}

read_int_any :: (p: *void, size: s64, signed: bool) -> string {
    if signed {
        v: s64;
        if size == {
            case 1; v = (cast(*s8)  p).*;
            case 2; v = (cast(*s16) p).*;
            case 4; v = (cast(*s32) p).*;
            case;   v = (cast(*s64) p).*;
        }
        return tprint("%", v);
    } else {
        v: u64;
        if size == {
            case 1; v = (cast(*u8)  p).*;
            case 2; v = (cast(*u16) p).*;
            case 4; v = (cast(*u32) p).*;
            case;   v = (cast(*u64) p).*;
        }
        return tprint("%", v);
    }
}
