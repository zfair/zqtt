use actix::prelude::*;
use log::{error, info};
use mqtt3::{self, Packet};
use std::io;
use std::net;
use std::time::Instant;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;

use super::codec;
use super::server;

pub struct Session {
    // store broker addr
    luid: u64,
    broker_addr: Addr<server::Broker>,
    socket_addr: net::SocketAddr,
    writer: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::MqttCodec>,

    // Session State
    started_at: Option<Instant>,
    connect_at: Option<Instant>,
}

impl Session {
    pub fn new(
        luid: u64,
        broker_addr: Addr<server::Broker>,
        socket_addr: net::SocketAddr,
        writer: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::MqttCodec>,
    ) -> Session {
        Session {
            luid: luid,
            broker_addr: broker_addr,
            socket_addr: socket_addr,
            writer: writer,

            started_at: None,
            connect_at: None,
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct MqttConnect(mqtt3::Connect);

/// To use `Framed` with an actor, we have to implement `StreamHandler` trait
impl StreamHandler<Result<Packet, io::Error>> for Session {
    /// This is main event loop for client requests
    fn handle(&mut self, msg: Result<Packet, io::Error>, ctx: &mut Self::Context) {
        match msg {
            Ok(packet) => {
                match packet {
                    // TODO: implement all mqtt packet type
                    Packet::Connect(_) => {
                        self.broker_addr
                            .send(server::SessionConnect {
                                session_id: self.luid,
                                session_addr: ctx.address(),
                            })
                            .into_actor(self)
                            .then(|res, act, ctx| {
                                match res {
                                    Ok(_) => {
                                        act.connect_at = Some(Instant::now());
                                        act.writer.write(Packet::Connack(mqtt3::Connack {
                                            session_present: false,
                                            code: mqtt3::ConnectReturnCode::Accepted,
                                        }));
                                    }
                                    Err(e) => {
                                        error!("e: {}", e);
                                        ctx.stop()
                                    }
                                }
                                fut::ready(())
                            })
                            .wait(ctx);
                    }
                    _ => unimplemented!(),
                }
            }
            Err(e) => {
                error!("e {}", e);
                unimplemented!()
            }
        }
    }
}

impl actix::io::WriteHandler<io::Error> for Session {}

impl Actor for Session {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("session started");
        self.started_at = Some(Instant::now())
    }
}
