use actix::prelude::*;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::net;
use std::str::FromStr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
use uuid::Uuid;

use crate::broker::session;
use crate::util::codec;
use crate::util::uid::{UidGen, UID};

pub struct Server {
    /// Local UID generator for this server.
    uid_gen: UidGen,

    /// Actor address of the corresponding broker.
    broker: Addr<Broker>,
}

impl Server {
    pub async fn run(addr: &String) -> Self {
        // Instantiate a local unique ID generator for all brokers.
        let uid_gen = UidGen::new();
        let broker = Broker::new(addr, uid_gen.to_owned()).await;

        Server { uid_gen, broker }
    }

    pub async fn stop(&self) -> Result<(), MailboxError> {
        self.broker.send(Stop()).await
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct TcpConnect(pub TcpStream, pub net::SocketAddr);

// Message for Session Notify Broker the Session Connect Info
#[derive(Message)]
#[rtype(result = "()")]
pub struct SessionConnect {
    pub session_id: UID,
    pub session_addr: Addr<session::Session>,
}

// Message for Session Notify Broker the Session Connect Info
#[derive(Message)]
#[rtype(result = "()")]
pub struct SessionDisConnect {
    session_id: UID,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

pub struct Broker {
    /// Network address for this broker.
    net_addr: std::net::SocketAddr,

    /// Local UID generator.
    uid_gen: UidGen,

    /// All client sessions in this broker.
    sessions: BTreeMap<UID, Addr<session::Session>>,
}

impl Actor for Broker {
    type Context = Context<Self>;
}

impl Handler<TcpConnect> for Broker {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) {
        let broker_addr = ctx.address();
        let luid = self.uid_gen.allocate();
        let guid = Uuid::new_v4();
        session::Session::create(move |session_ctx| {
            let (r, w) = tokio::io::split(msg.0);
            session::Session::add_stream(FramedRead::new(r, codec::MqttCodec), session_ctx);
            session::Session::new(
                luid,
                guid,
                broker_addr,
                msg.1,
                actix::io::FramedWrite::new(w, codec::MqttCodec, session_ctx),
            )
        });
    }
}

impl Handler<SessionConnect> for Broker {
    type Result = ();

    fn handle(&mut self, msg: SessionConnect, _ctx: &mut Context<Self>) {
        self.sessions.insert(msg.session_id, msg.session_addr);
    }
}

impl Handler<Stop> for Broker {
    type Result = ();

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<Self>) {
        ctx.stop()
    }
}

impl Broker {
    pub async fn new(addr: &String, uid_gen: UidGen) -> Addr<Self> {
        let net_addr = net::SocketAddr::from_str(addr).unwrap();
        let listener = Box::new(TcpListener::bind(&net_addr).await.unwrap());

        Broker::create(move |ctx| {
            // add_message_stream require a static lifetime stream
            // Box::leak cast to static references
            let tcp_connect = Box::leak(listener).incoming().map(move |st| {
                let st = st.unwrap();
                let addr = st.peer_addr().unwrap();
                TcpConnect(st, addr)
            });
            ctx.add_message_stream(tcp_connect);
            Broker {
                net_addr,
                uid_gen,
                sessions: BTreeMap::new(),
            }
        })
    }
}
