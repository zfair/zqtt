use actix::prelude::*;
use log::{error, info};
use mqtt3::{self, Connack, Connect, ConnectReturnCode, Packet};
use std::io;
use std::net;
use std::time::Instant;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;

use super::codec;
use super::server;
use super::message;
use super::util::UID;

pub struct Session {
    /// Local UID of this session.
    uid: UID,

    /// Actor address of this session.
    addr: Option<Addr<Self>>,

    /// Actor address of the corresponding broker.
    broker_addr: Addr<server::Broker>,

    /// Session's network address.
    net_addr: net::SocketAddr,

    /// Framed writer for MQTT packets.
    writer: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::MqttCodec>,

    /// Session timestamps.
    started_at: Option<Instant>,
    connect_at: Option<Instant>,
}

impl Session {
    pub fn new(
        uid: UID,
        broker_addr: Addr<server::Broker>,
        socket_addr: net::SocketAddr,
        writer: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::MqttCodec>,
    ) -> Session {
        Session {
            uid,
            addr: None,
            broker_addr,
            net_addr: socket_addr,
            writer,
            started_at: None,
            connect_at: None,
        }
    }

    fn on_connect(
        &mut self,
        _conn: &Connect,
        ctx: &mut <Self as Actor>::Context,
    ) -> impl ActorFuture<Output=Result<Packet, MailboxError>, Actor=Self> {
        let f = self
            .broker_addr
            .send(server::SessionConnect {
                session_id: self.uid,
                session_addr: ctx.address(),
            })
            .into_actor(self)
            .then(move |res, act, ctx| match res {
                Ok(_) => {
                    act.connect_at = Some(Instant::now());
                    fut::ok(Packet::Connack(Connack {
                        session_present: false,
                        code: ConnectReturnCode::Accepted,
                    }))
                }
                Err(e) => {
                    error!("error: {}", e);
                    ctx.stop();
                    fut::err(e)
                }
            });
        f
    }
}

/// To use `Framed` with an actor, we have to implement `StreamHandler` trait
impl StreamHandler<Result<Packet, io::Error>> for Session {
    /// This is the main event loop for client requests
    fn handle(&mut self, msg: Result<Packet, io::Error>, ctx: &mut Self::Context) {
        match msg {
            Ok(packet) => {
                let res = match packet {
                    // TODO: implement all mqtt packet type
                    Packet::Connect(m) => self.on_connect(&m, ctx),
                    _ => unimplemented!(),
                };
                res.then(move |res, act, ctx| {
                    match res {
                        Ok(m) => {
                            act.writer.write(m);
                        }
                        Err(e) => {
                            error!("error: {}", e);
                            ctx.stop()
                        }
                    }
                    fut::ready(())
                })
                    .wait(ctx)
            }
            Err(e) => {
                error!("error: {}", e);
                unimplemented!()
            }
        }
    }
}

impl actix::io::WriteHandler<io::Error> for Session {}

impl Actor for Session {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("session started");
        self.addr = Some(ctx.address());
        self.started_at = Some(Instant::now());
    }
}

impl message::Subscriber for Session {
    fn id(&self) -> String {
        self.uid.to_string()
    }

    fn kind(&self) -> message::SubscriberKind {
        message::SubscriberKind::Local
    }

    fn addr(&self) -> Option<Addr<Self>> {
        self.addr.clone()
    }
}
