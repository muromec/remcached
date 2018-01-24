use std::str;
use std::collections::HashMap;
use std::hash::Hash;
use nom::*;

#[derive(Debug, PartialEq, Eq)]
enum Method {
    GET,
    SET,
}

#[derive(Debug)]
pub struct Request {
    command: Method,
    key: String,
    rest: String,
    body: String,
}

named!(parse_command<&[u8], Method>,
    alt!(
        map!(tag!("get"), |_| Method::GET) |
        map!(tag!("set"), |_| Method::SET)
    )
);

named!(pub parse_request<&[u8], Request>, ws!(do_parse!(
    command: parse_command >>
    key: map_res!(take_until!(" "), str::from_utf8) >>
    rest: map_res!(take_until!("\n"), str::from_utf8) >>
    body_r: cond!(command == Method::SET, map_res!(take_until!("\n"), str::from_utf8)) >>
    (Request { 
        command: command,
        key: key.into(),
        rest: rest.into(),
        body: match body_r {
            Some(body) => body.into(),
            None => String::new(),
        },
    })
)));

pub fn handle(command: Request, storage: &mut HashMap<String, String>) -> Vec<u8> {
    match command.command {
        Method::GET => {
            match storage.get(command.key.as_str()) {
                Some(response)=> return response.clone().into_bytes(),
                None=> {},
            };
        },
        Method::SET => {
            storage.insert(command.key, command.body);
        },
    }
    return vec![111, 107, 10];
}

pub fn parse(buf: &[u8]) -> Option<Request> {
    return match parse_request(buf) {
        IResult::Done(_raw, command)=> Option::Some(command),
        IResult::Error(er)=> Option::None,
        IResult::Incomplete(_ingore)=> Option::None,
    }
}
