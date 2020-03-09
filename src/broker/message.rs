use bytes::Bytes;
use actix::prelude::*;
use std::sync::Arc;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Message {
    id: Bytes,
    channel: Bytes,
    payload: Bytes,
    ttl: u32,
}

pub enum SubscriberType {
    Direct,
    Remote,
}

pub trait Subscriber {
    fn id(&self) -> String;
    fn subscriber_type(&self) -> SubscriberType;
    fn subscriber_addr(&self) -> Option<Addr<Self>>
    where
        Self: Actor;
}
