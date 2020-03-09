use bytes::Bytes;
use std::sync::Arc;

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
    fn send(&mut self, m: Arc<Message>);
}
