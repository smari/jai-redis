// jai-redis
// Redis bindings for Jai; RESP tools.
// Smári McCarthy <smari@smarimccarthy.is>
BUFFER_MAX :: 1024;

RESPType :: enum u8 {
    ERROR           :: #char "-";
    INTEGER         :: #char ":";
    SIMPLE_STRING   :: #char "+";
    BULK_STRING     :: #char "$";
    ARRAY           :: #char "*";
    NULL            :: 0;
}

RESPMessage :: struct {
    type  : RESPType;
    value : union { s: string; i: s64; };
}

to_string :: (resp: *RESPMessage) -> string {
    if resp.type == {
        case .SIMPLE_STRING; return resp.value.s;
        case .BULK_STRING; return resp.value.s;
        case .ERROR; return resp.value.s;
        case .INTEGER; return tprint("%", resp.value.i);
        case .NULL; return "Null";
        case; return "What?";
    }
}

resp_parse_simple_string :: (resp: *RESPMessage, buf: *u8) {
    len := 0;
    while buf[len+1] != cast(u8) #char "\r" len += 1;
    resp.value.s.data = alloc(len);
    memcpy(resp.value.s.data, buf+1, len);
    resp.value.s.count = len;
}

resp_parse_error :: (resp: *RESPMessage, buf: *u8) {
    len := 0;
    while buf[len+1] != cast(u8) #char "\r" len += 1;
    resp.value.s.data = alloc(len);
    memcpy(resp.value.s.data, buf+1, len);
    resp.value.s.count = len;
}

resp_parse_integer :: (resp: *RESPMessage, buf: *u8) {
    s : string;
    s.data = buf+1;
    while s.data[s.count] != cast(u8) #char "\r" s.count += 1;
    resp.value.i = parse_int(*s, s64);
}

resp_parse_bulk_string :: (resp: *RESPMessage, buf: *u8) {
    p := buf + 1;
    length := 0;
    if p[0] == cast(u8) #char "-" {
        resp.type = .NULL;
        return;
    }
    while(p[0] != cast(u8) #char "\r") {
        length = (length*10)+(p[0] - cast(u8)#char "0");
        p += 1;
    }
    p += 2;
    resp.value.s.data = alloc(length);
    resp.value.s.count = length;
    memcpy(resp.value.s.data, p, length);
}

resp_parse_array :: (resp: *RESPMessage, buf: *u8) {
    p := buf;
    length := 0;
    if p[0] == cast(u8) #char "-" {
        resp.type = .NULL;
        return;
    }
    while(p[0] != cast(u8) #char "\r") {
        length = (length*10)+(p[0] - cast(u8)#char "0");
        p += 1;
    }
}

resp_parse :: (socket: s32) -> RESPMessage, bool {
    // TODO: Handle bulk strings and arrays of length > BUFFER_MAX.
    resp : RESPMessage;
    buf : *u8 = alloc(BUFFER_MAX);
    memset(buf, 0, BUFFER_MAX);
    n := read(socket, buf, BUFFER_MAX);
    if n < 0 { print("error reading from socket"); return resp, false; }
    index := 0;


    s : string;
    s.data = buf;
    s.count = n;
    //print("RESP: >>>%<<<\n", s);

    if buf[index] == {
        case #char "+";
            resp.type = .SIMPLE_STRING;
            resp_parse_simple_string(*resp, buf);
        case #char "-";
            resp.type = .ERROR;
            resp_parse_error(*resp, buf);
        case #char ":";
            resp.type = .INTEGER;
            resp_parse_integer(*resp, buf);
        case #char "$";
            resp.type = .BULK_STRING;
            resp_parse_bulk_string(*resp, buf);
        case #char "*";
            resp.type = .ARRAY;
            resp_parse_array(*resp, buf);
        case; return resp, false;
    }

    return resp, true;
}

#scope_file
#import "Basic";
#import "String";
#import "Socket";
#import "POSIX";
