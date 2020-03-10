use actix::prelude::*;
use bytes::Bytes;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Message {
    id: Bytes,
    channel: Bytes,
    payload: Bytes,
    ttl: u32,
}

pub enum SubscriberKind {
    Local,
    Remote,
}

pub trait Subscriber {
    fn id(&self) -> String;
    fn kind(&self) -> SubscriberKind;
    fn addr(&self) -> Option<Addr<Self>>
    where
        Self: Actor;
}
