use super::codec;
use super::session;
use actix::prelude::*;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::net;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;

pub struct Server {
    // local unique id generator for this broker
    luid_gen: Arc<AtomicU64>,
    broker: Addr<Broker>,
}

impl Server {
    pub async fn run(addr: &str) -> Self {
        // instance a local unique id generator for all broker
        let luid_gen = Arc::new(AtomicU64::new(1));
        let broker = Broker::new(addr, luid_gen.clone()).await;

        Server { luid_gen, broker }
    }

    pub async fn stop(&self) -> Result<(), MailboxError> {
        // send stop message to broker
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
    pub session_id: u64,
    pub session_addr: Addr<session::Session>,
}

// Message for Session Notify Broker the Session Connect Info
#[derive(Message)]
#[rtype(result = "()")]
pub struct SessionDisConnect {
    session_id: u64,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Stop();

pub struct Broker {
    addr: std::net::SocketAddr,
    luid_gen: Arc<AtomicU64>,
    // store all clients
    sessions: BTreeMap<u64, Addr<session::Session>>,
}

impl Actor for Broker {
    type Context = Context<Self>;
}

impl Handler<TcpConnect> for Broker {
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, ctx: &mut Context<Self>) {
        let broker_addr = ctx.address();
        let luid = self.luid_gen.fetch_add(1, Ordering::SeqCst);
        session::Session::create(move |session_ctx| {
            let (r, w) = tokio::io::split(msg.0);
            session::Session::add_stream(FramedRead::new(r, codec::MqttCodec), session_ctx);
            session::Session::new(
                luid,
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
    pub async fn new(addr: &str, luid_gen: Arc<AtomicU64>) -> Addr<Self> {
        let addr = net::SocketAddr::from_str(addr).unwrap();
        let listener = Box::new(TcpListener::bind(&addr).await.unwrap());

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
                addr,
                luid_gen,
                sessions: BTreeMap::new(),
            }
        })
    }
}
