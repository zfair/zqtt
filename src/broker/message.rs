use std::sync::Arc;
use bytes::Bytes;

pub struct Message {
    id:  Bytes,
    channel: Bytes,
    payload: Bytes,
    ttl: u32,
}

pub enum SubscriberType {
    SubscriberDirect,
    SubscriberRemote,
}

pub trait Subscriber {
    fn id(&self) -> String;
    fn subscriber_type(&self) -> SubscriberType;
    fn send(&mut self, m: Arc<Message>);
}
