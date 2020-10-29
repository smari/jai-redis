// jai-redis
// Redis bindings for Jai
// Smári McCarthy <smari@smarimccarthy.is>
#load "resp.jai";

Redis :: struct {
    socket:     s32;
    host:       string;
    port:       u16;

}

init :: (redis: *Redis, host: string, port: u16) -> bool {
    servaddr, cli : sockaddr_in;

    redis.host = host;
    redis.port = port;
    redis.socket = socket(AF_INET, SOCK_STREAM, 0);
    if redis.socket == -1 {
        print("socket creation failed...\n");
        return false;
    }

    memset(*servaddr, 0, size_of(sockaddr_in));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    addr := to_c_string(host);
    servaddr.sin_addr = inet_addr(addr);
    servaddr.sin_port = htons(port);

    // connect the client socket to server socket
    if (connect(redis.socket, xx *servaddr, xx size_of(sockaddr_in)) != 0) {
        print("connection with the server failed...\n");
        return false;
    }

    return true;
}

deinit :: (redis: *Redis) {
    close(redis.socket);
}


redis_send :: (redis: *Redis, msg: .. Any) -> RESPMessage, bool {
    write(redis.socket, tprint("*%\r\n", msg.count));
    for a:msg {
        if a.type.type == {
            case .INTEGER;
                s := tprint("%", << cast(*int)a.value_pointer);
                write(redis.socket, tprint("$%\r\n", s.count));
                write(redis.socket, tprint("%\r\n", s));
            case .STRING;
                s := << cast(*string)a.value_pointer;
                write(redis.socket, tprint("$%\r\n", s.count));
                write(redis.socket, s);
                write(redis.socket, "\r\n");
        }
    }
    resp : RESPMessage;

    resp = resp_parse(redis.socket);
    return resp, false;
}


redis_info :: (redis: *Redis) {
    res := redis_send(redis, "INFO");
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
    if ret.type == .ERROR return ret.value.s, false;
    if ret.type == .NULL return "", false;
    return ret.value.s, true;
}


redis_getset :: (redis: *Redis, key: string, value: Any, ex:=0, px:=0, nx:=false, _xx:=false) -> string, bool {
    suc, ret := redis_set(redis, key, value, ex, px, nx, _xx, getset=true);
    if ret.type == .ERROR return ret.value.s, false;
    if ret.type == .NULL return "", false;
    return ret.value.s, true;
}


redis_strlen :: (redis: *Redis, key: string) -> s64 {
    ret, suc := redis_send(redis, "STRLEN", key);
    return ret.value.i;
}


redis_append :: (redis: *Redis, key: string, append: string) -> bool {
    ret, suc := redis_send(redis, "APPEND", key, append);
    return suc;
}


redis_getrange :: (redis: *Redis, key: string, start: int, end: int) -> string, bool {
    ret, suc := redis_send(redis, "GETRANGE", key, start, end);
    return ret.value.s, suc;
}


redis_setrange :: (redis: *Redis, key: string, start: int, value: Any) -> bool {
    ret, suc := redis_send(redis, "SETRANGE", key, start, value);
    return suc;
}



#scope_file
#import "Socket";
