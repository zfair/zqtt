use actix::prelude::*;
use std::io::{self, ErrorKind, Cursor};
use tokio::io::WriteHalf;
use tokio::net::{TcpStream};
use std::net;
use mqtt3::{self, Packet};

use super::server;
use super::codec;


pub struct Session {
    // store broker addr
    luid: u64,
    broker_addr: Addr<server::Broker>,
    socket_addr: net::SocketAddr,
    writer: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::MqttCodec>,
}

impl Session{
    /*  */
    pub fn new(
        luid: u64,
        broker_addr: Addr<server::Broker>,
        socket_addr: net::SocketAddr,
        writer: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::MqttCodec>,
    ) -> Session {
        Session{
            luid: luid,
            broker_addr: broker_addr,
            socket_addr: socket_addr,
            writer: writer,
        }
    }
}

/// To use `Framed` with an actor, we have to implement `StreamHandler` trait
impl StreamHandler<Result<Packet, io::Error>> for Session {
    /// This is main event loop for client requests
    fn handle(&mut self, msg: Result<Packet, io::Error>, ctx: &mut Self::Context) {
        match msg {
            Ok(packet) => {
                match packet {
                    // TODO: implement all mqtt packet type
                    Packet::Connect(connect) => unimplemented!(),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

impl actix::io::WriteHandler<io::Error> for Session {}

impl Actor for Session {
    type Context = Context<Self>;
}