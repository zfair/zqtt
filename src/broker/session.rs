use actix::prelude::*;
use actix::fut::IntoActorFuture;
use actix::fut::FutureWrap;
use log::{error, info};
use mqtt3::{self, Packet};
use std::pin::Pin;
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

    fn on_connect(&mut self, m: &mqtt3::Connect, ctx: &mut <Self as Actor>::Context) -> impl ActorFuture<Output = Result<mqtt3::Packet, MailboxError>, Actor = Self> {
        let f = self.broker_addr
            .send(server::SessionConnect {
                session_id: self.luid,
                session_addr: ctx.address(),
            })
            .into_actor(self)
            .then(|res, act, ctx| {
                match res {
                    Ok(_) => {
                        act.connect_at = Some(Instant::now());
                        fut::ok(Packet::Connack(mqtt3::Connack {
                            session_present: false,
                            code: mqtt3::ConnectReturnCode::Accepted,
                        }))
                        
                    }
                    Err(e) => {
                        error!("e: {}", e);
                        ctx.stop();
                        fut::err(e)
                    }
                }
            });
            f
    }
}

/// To use `Framed` with an actor, we have to implement `StreamHandler` trait
impl StreamHandler<Result<Packet, io::Error>> for Session {
    /// This is main event loop for client requests
    fn handle(&mut self, msg: Result<Packet, io::Error>, ctx: &mut Self::Context) {
        match msg {
            Ok(packet) => {
                let res = match packet {
                    // TODO: implement all mqtt packet type
                    Packet::Connect(m) => {
                        self.on_connect(&m, ctx)
                    },
                    _ => unimplemented!(),
                };
                res.then(|res, act, ctx| {
                    match res {
                        Ok(m) => {
                            act.writer.write(m);
                        },
                        Err(e) => {
                            error!("e: {}", e);
                            ctx.stop()
                        }
                    }
                    fut::ready(())
                }).wait(ctx)
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
