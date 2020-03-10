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

/// The location of the subscriber, outside the node or just on the node.
pub enum SubLocation {
    Local,
    Remote,
}

pub trait Subscriber {
    fn id(&self) -> String;
    fn location(&self) -> SubLocation;
    fn addr(&self) -> Option<Addr<Self>>
    where
        Self: Actor;
}
