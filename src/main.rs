extern crate mio;
extern crate bytes;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate nom;

use mio::{EventLoop, Handler, Token, EventSet, PollOpt, TryRead, TryWrite};
use mio::tcp::*;
use mio::util::Slab;

use bytes::{Buf};
use std::io::Cursor;
use std::collections::HashMap;

mod proto;

#[derive(Debug)]
enum State {
    Reading(Vec<u8>),
    Writing(Cursor<Vec<u8>>),
    Closed,
}

impl State {
    fn mut_read_buf(&mut self) -> &mut Vec<u8> {
        match *self {
            State::Reading(ref mut buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    fn read_buf(&self) -> &[u8] {
        match *self {
            State::Reading(ref buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    fn write_buf(&self) -> &Cursor<Vec<u8>> {
        match *self {
            State::Writing(ref buf) => buf,
            _ => panic!("connection not in writing state"),
        }
    }

    fn mut_write_buf(&mut self) -> &mut Cursor<Vec<u8>> {
        match *self {
            State::Writing(ref mut buf) => buf,
            _ => panic!("connection not in Writing state"),
        }
    }

    fn parse_command(&self) -> Option<proto::Request> {
        return proto::parse(self.read_buf());
    }

    fn transition_to_writing(&mut self, buf: Vec<u8>) {
        *self = State::Writing(Cursor::new(buf));
    }

    fn try_transition_to_reading(&mut self) {
        if !self.write_buf().has_remaining() {
            *self = State::Reading(Vec::new());
        }
    }
}

#[derive(Debug)]
struct Connection {
    socket: TcpStream,
    token: Token,
    state: State,
}

impl Connection {
    fn new(socket: TcpStream, token: Token) -> Connection {
        Connection {
            socket: socket,
            token: token,
            state: State::Reading(vec![]),
        }
    }

    fn ready(&mut self, event_loop: &mut EventLoop<Remcached>, events: EventSet) -> Option<proto::Request> {
        debug!("  connection state=:{:?}", self.state);

        match self.state {
            State::Reading(..) => {
                assert!(events.is_readable(), "unexpected events; events={:?}", events);
                return self.read(event_loop);
            }
            State::Writing(..) => {
                assert!(events.is_writable(), "unexpected events; events={:?}", events);
                self.write(event_loop)
            }
            _ => unimplemented!(),
        }
        return Option::None;
    }

    fn read(&mut self, event_loop: &mut EventLoop<Remcached>)-> Option<proto::Request> {
        match self.socket.try_read_buf(self.state.mut_read_buf()) {
            Ok(Some(0)) => {
                debug!("    read 0 bytes from client; buffered={}", self.state.read_buf().len());
            }
            Ok(Some(n)) => {
                debug!("read {} bytes", n);
                self.reregister(event_loop);
            }
            Ok(None) => {
                debug!("read nothing");
                self.reregister(event_loop);
            }
            Err(e) => {
                self.state = State::Closed;
                panic!("got an error trying to read; err={:?}", e);
            }
        }
        return self.state.parse_command();
    }

    fn reply(&mut self, event_loop: &mut mio::EventLoop<Remcached>, buf: Vec<u8>) {
        debug!("reply");
        self.state.transition_to_writing(buf);
        self.reregister(event_loop);
    }

    fn write(&mut self, event_loop: &mut mio::EventLoop<Remcached>) {
        match self.socket.try_write_buf(self.state.mut_write_buf()) {
            Ok(Some(_)) => {
                self.state.try_transition_to_reading();
                self.reregister(event_loop);
            }
            Ok(None) => {
                self.state.try_transition_to_reading();
                self.reregister(event_loop);
            }
            Err(e) => {
                panic!("got an error trying to write; err={:?}", e);
            }
        }
    }

    fn reregister(&self, event_loop: &mut EventLoop<Remcached>) {
        let event_set = match self.state {
            State::Reading(..) => EventSet::readable(),
            State::Writing(..) => EventSet::writable(),
            _ => EventSet::none(),
        };

        // Why unwrap to make sure it is OK???
        event_loop.reregister(&self.socket, self.token, event_set, PollOpt::oneshot()).unwrap();
    }

    fn is_closed(&self) -> bool {
        match self.state {
            State::Closed => true,
            _ => false,
        }
    }
}

struct Remcached {
    server: TcpListener,
    connections: Slab<Connection>,
    storage: HashMap<String, String>,
}

impl Remcached {
    fn new(server: TcpListener) -> Remcached {
        let slab = Slab::new_starting_at(Token(1), 1024);

        Remcached {
            server: server,
            connections: slab,
            storage: HashMap::new(),
        }
    }
}

const SERVER: Token = Token(0);

impl Handler for Remcached {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token,
             events: EventSet) {

        debug!("Token: {:?}", token);
        match token {
            SERVER =>  {
                info!("the server socket is ready to accept connection");
                match self.server.accept() {
                    Ok(Some((socket, _))) => {
                        debug!("accepted a socket");

                        let token = self.connections
                            .insert_with(|token| Connection::new(socket, token))
                            .unwrap();

                        event_loop.register(
                            &self.connections[token].socket,
                            token,
                            EventSet::readable(),
                            PollOpt::edge() | PollOpt::oneshot()).unwrap();
                    }
                    Ok(None) => {
                        warn!("the server socket wasn't actually ready")
                    }
                    Err(e) => {
                        error!("listener.accept() error: {}", e);
                        event_loop.shutdown();
                    }
                }
            }
            _ => {
                let res = self.connections[token].ready(event_loop, events);
                match res {
                    Some(command) => {
                        self.connections[token].reply(
                            event_loop, proto::handle(command, &mut self.storage)
                        );
                    },
                    None => {},
                }

                if self.connections[token].is_closed() {
                    let _ = self.connections.remove(token);
                }
            }
        }
    }
}

fn main()
{
    env_logger::init();
    let server = TcpListener::bind(&"127.0.0.1:9922".parse().unwrap()).unwrap();

    let mut e = EventLoop::new().unwrap();

    e.register(&server, SERVER, EventSet::readable(), PollOpt::edge()).unwrap();

    info!("running remcache server");

    let mut remcached = Remcached::new(server);

    e.run(&mut remcached).ok().expect("Failed to start event loop");
}
