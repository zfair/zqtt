use actix::prelude::*;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::ptr::NonNull;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub enum SubscribeError {}

impl fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl error::Error for SubscribeError {}

#[derive(Debug, Clone, Message)]
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
    fn send(&self, m: &Message) -> Result<(), SubscribeError>;
}

struct Node {
    chan_id: Option<ChanID>,
    parent: Option<NonNull<Node>>,
    children: BTreeMap<ChanID, Option<NonNull<Node>>>,
    subs: HashMap<String, Rc<dyn Subscriber>>,
}

impl Default for Node {
    fn default() -> Self {
        Node {
            chan_id: None,
            parent: None,
            children: BTreeMap::default(),
            subs: HashMap::default(),
        }
    }
}

/// Channel ID, namely the hash of a topic channel.
type ChanID = u64;

/// Subscription ID, a list of [ChanID]s.
type SubsID = Vec<ChanID>;

#[derive(Debug)]
pub struct SubTrie {
    root: Option<NonNull<Node>>,
}

impl SubTrie {
    fn new() -> Self {
        let boxed_node = Box::new(Node::default());
        SubTrie {
            root: Some(Box::into_raw_non_null(boxed_node)),
        }
    }

    fn subscribe(
        &mut self,
        subs_id: &SubsID,
        subscriber: Rc<dyn Subscriber>,
    ) -> Result<(), SubscribeError> {
        let mut cur = self.root;

        for chan_id in subs_id.iter() {
            match cur {
                Some(mut n) => {
                    let child;
                    unsafe {
                        child = n.as_mut();
                    }
                    let next = child.children.get_mut(chan_id);
                    match next {
                        Some(x) => cur = *x,
                        None => {
                            let mut new_node = Box::into_raw_non_null(Box::new(Node::default()));
                            unsafe {
                                new_node.as_mut().parent = cur;
                                new_node.as_mut().chan_id = Some(*chan_id);
                            }
                            child.children.insert(*chan_id, Some(new_node));
                            cur = Some(new_node);
                        }
                    }
                }
                None => unreachable!(),
            }
        }
        unsafe {
            cur.unwrap()
                .as_mut()
                .subs
                .insert(subscriber.id(), subscriber.to_owned());
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestSubscriber {
        id: String,
    }

    impl TestSubscriber {
        fn new(id: String) -> Self {
            TestSubscriber { id }
        }
    }

    impl Subscriber for TestSubscriber {
        fn id(&self) -> String {
            self.id.to_owned()
        }
        fn kind(&self) -> SubscriberKind {
            SubscriberKind::Local
        }
        fn send(&self, m: &super::Message) -> Result<(), SubscribeError> {
            Ok(())
        }
    }

    #[test]
    fn test_subscribe() {
        let mut sub_trie = SubTrie::new();
        let subs_id_test = (0..10).collect();
        let subscriber_test = Rc::new(TestSubscriber::new("Test".to_string()));
        let subs_id_test2 = (0..10).collect();
        let subscriber_test2 = Rc::new(TestSubscriber::new("Test2".to_string()));
        let subs_id_test3 = (0..11).collect();
        let subscriber_test3 = Rc::new(TestSubscriber::new("Test3".to_string()));

        sub_trie
            .subscribe(&subs_id_test, subscriber_test.to_owned())
            .unwrap();
        sub_trie
            .subscribe(&subs_id_test2, subscriber_test2.to_owned())
            .unwrap();
        sub_trie
            .subscribe(&subs_id_test3, subscriber_test3.to_owned())
            .unwrap();

        {
            let mut cur = sub_trie.root;
            for x in subs_id_test {
                unsafe {
                    cur = *cur.unwrap().as_mut().children.get(&x).unwrap();
                    assert_eq!(cur.unwrap().as_mut().chan_id, Some(x));
                }
            }

            unsafe {
                assert_eq!(
                    cur.unwrap().as_mut().subs["Test"].id(),
                    subscriber_test.id()
                );
                assert_eq!(
                    cur.unwrap().as_mut().subs["Test2"].id(),
                    subscriber_test2.id()
                );
                assert_eq!(cur.unwrap().as_mut().subs.len(), 2);
            }
        }

        {
            let mut cur = sub_trie.root;
            for x in subs_id_test3 {
                unsafe {
                    cur = *cur.unwrap().as_mut().children.get(&x).unwrap();
                }
            }

            unsafe {
                assert_eq!(
                    cur.unwrap().as_mut().subs["Test3"].id(),
                    subscriber_test3.id()
                );
                assert_eq!(cur.unwrap().as_mut().subs.len(), 1);
                assert_eq!(
                    cur.unwrap().as_mut().parent.unwrap().as_mut().subs["Test"].id(),
                    subscriber_test.id()
                );
                assert_eq!(
                    cur.unwrap().as_mut().parent.unwrap().as_mut().subs["Test2"].id(),
                    subscriber_test2.id()
                );
                assert_eq!(
                    cur.unwrap().as_mut().parent.unwrap().as_mut().subs["Test2"].id(),
                    subscriber_test2.id()
                );
                assert_eq!(cur.unwrap().as_mut().parent.unwrap().as_mut().subs.len(), 2);
            }
        }
    }
}
