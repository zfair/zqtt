use actix::prelude::*;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::ptr::NonNull;
use std::rc::Rc;

/// Channel ID, namely the hash of a topic channel.
type ChanID = u64;

/// Subscription ID, a list of [ChanID]s.
type SubsID = Vec<ChanID>;

// hash_map::DefaultHasher hash value of “+”
const SINGLE_WILDCARD: ChanID = 7874756943448743542;
// hash_map::DefaultHasher hash value of "#"
const MULTIPLE_WILDCARD: ChanID = 5913179443045906980;

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

type Subscribers = HashMap<String, Rc<dyn Subscriber>>;
fn subscribers_add_range(dst: &mut Subscribers, from: &Subscribers) {
    for (key, val) in from.iter() {
        dst.insert(key.to_string(), val.clone());
    }
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
    subs: Subscribers,
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

    pub fn subscribe(
        &mut self,
        ssid: &SubsID,
        subscriber: Rc<dyn Subscriber>,
    ) -> Result<(), SubscribeError> {
        let mut cur = self.root;

        for chan_id in ssid.iter() {
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
                .insert(subscriber.id(), subscriber.clone());
        }
        Ok(())
    }

    pub fn lookup(&self, ssid: &Vec<u64>) -> Result<Subscribers, SubscribeError> {
        let mut subs = Subscribers::new();
        self.slookup(&self.root, ssid.as_slice(), &mut subs)?;
        Ok(subs)
    }

    fn slookup(
        &self,
        node: &Option<NonNull<Node>>,
        ssid: &[u64],
        subs: &mut Subscribers,
    ) -> Result<(), SubscribeError> {
        if ssid.len() == 0 {
            // find match node
            unsafe {
                let this_node = &*node.unwrap().as_ptr();
                let s = &this_node.subs;
                subscribers_add_range(subs, s);
                // search MULTIPLE_WILDCARD at last level
                if let Some(mwcn) = this_node.children.get(&MULTIPLE_WILDCARD) {
                    subscribers_add_range(subs, &(*mwcn.unwrap().as_ptr()).subs);
                }
            }
            return Ok(());
        }

        let this_node;
        unsafe {
            this_node = &*node.unwrap().as_ptr();
        }

        if let Some(mwcn) = this_node.children.get(&MULTIPLE_WILDCARD) {
            unsafe {
                subscribers_add_range(subs, &(*mwcn.unwrap().as_ptr()).subs);
            }
        }
        // dfs lookup single wildcard
        if let Some(swcn) = this_node.children.get(&SINGLE_WILDCARD) {
            self.slookup(swcn, &ssid[1..ssid.len()], subs)?;
        }
        if let Some(matchn) = this_node.children.get(&ssid[0]) {
            self.slookup(matchn, &ssid[1..ssid.len()], subs)?;
        }

        Ok(())
    }
}

#[cfg(test)]
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(test)]
    struct TestSubscriber {
        id: String,
    }

    #[cfg(test)]
    impl TestSubscriber {
        fn new(id: String) -> Self {
            TestSubscriber { id: id }
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    fn parse_topic(topic: String) -> Vec<u64> {
        let parts: Vec<&str> = topic.split("/").collect();
        let mut result = Vec::new();
        for part in parts.iter() {
            let mut hasher = DefaultHasher::new();
            part.hash(&mut hasher);
            let p = hasher.finish();
            result.push(p);
        }
        result
    }

    #[test]
    fn test_subscribe() {
        let mut sub_trie = SubTrie::new();
        let ssid_test: Vec<u64> = (0..10).collect();
        let subscriber_test = Rc::new(TestSubscriber::new("Test".to_string()));
        let ssid_test2: Vec<u64> = (0..10).collect();
        let subscriber_test2 = Rc::new(TestSubscriber::new("Test2".to_string()));
        let ssid_test3: Vec<u64> = (0..11).collect();
        let subscriber_test3 = Rc::new(TestSubscriber::new("Test3".to_string()));

        sub_trie
            .subscribe(&ssid_test, subscriber_test.to_owned())
            .unwrap();
        sub_trie
            .subscribe(&ssid_test2, subscriber_test2.to_owned())
            .unwrap();
        sub_trie
            .subscribe(&ssid_test3, subscriber_test3.to_owned())
            .unwrap();

        {
            let mut cur = sub_trie.root;
            for x in ssid_test {
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
            for x in ssid_test3 {
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

    #[cfg(test)]
    struct LookupTestCase {
        lookup_topic: String,
        match_count: usize,
        match_ids: Vec<String>,
    }

    #[test]
    fn test_lookup() {
        let topics = vec![
            "#",
            "+",
            "hello/#",
            "hello/+",
            "hello/+/zqtt",
            "hello/mqtt/#",
            "hello/mqtt/+",
            "hello/mqtt/zqtt",
            "hello/mqtt/+/+",
            "hello/mqtt/+/foo",
            "hello/mqtt/zqtt/foo",
        ];

        let lookups: Vec<LookupTestCase> = vec![
            LookupTestCase {
                lookup_topic: "a".to_string(),
                match_count: 2,
                match_ids: vec!["#".to_string(), "+".to_string()],
            },
            LookupTestCase {
                lookup_topic: "a/b".to_string(),
                match_count: 1,
                match_ids: vec!["#".to_string()],
            }, // "x",
            LookupTestCase {
                lookup_topic: "x/y".to_string(),
                match_count: 1,
                match_ids: vec!["#".to_string()],
            },
            LookupTestCase {
                lookup_topic: "hello/world".to_string(),
                match_count: 3,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/+".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/world/c".to_string(),
                match_count: 2,
                match_ids: vec!["#".to_string(), "hello/#".to_string()],
            },
            LookupTestCase {
                lookup_topic: "hello/world/zqtt".to_string(),
                match_count: 3,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/+/zqtt".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/mqtt/zqtt".to_string(),
                match_count: 6,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/+/zqtt".to_string(),
                    "hello/mqtt/+".to_string(),
                    "hello/mqtt/zqtt".to_string(),
                    "hello/mqtt/#".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/mqtt/ohh".to_string(),
                match_count: 4,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/mqtt/#".to_string(),
                    "hello/mqtt/+".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/mqtt/ohh/bili".to_string(),
                match_count: 4,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/mqtt/#".to_string(),
                    "hello/mqtt/+/+".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/mqtt/bili/acfun".to_string(),
                match_count: 4,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/mqtt/#".to_string(),
                    "hello/mqtt/+/+".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/mqtt/bili/foo".to_string(),
                match_count: 5,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/mqtt/#".to_string(),
                    "hello/mqtt/+/+".to_string(),
                    "hello/mqtt/+/foo".to_string(),
                ],
            },
            LookupTestCase {
                lookup_topic: "hello/mqtt/zqtt/foo".to_string(),
                match_count: 6,
                match_ids: vec![
                    "#".to_string(),
                    "hello/#".to_string(),
                    "hello/mqtt/#".to_string(),
                    "hello/mqtt/+/+".to_string(),
                    "hello/mqtt/+/foo".to_string(),
                    "hello/mqtt/zqtt/foo".to_string(),
                ],
            },
        ];

        let parsed_topics: Vec<Vec<u64>> = topics
            .clone()
            .into_iter()
            .map(|x| parse_topic(x.to_string()))
            .collect();
        let mut sub_trie = SubTrie::new();
        for (pos, topic) in parsed_topics.iter().enumerate() {
            // use topic name as subscriber id
            let test_subscriber = Rc::new(TestSubscriber::new(topics[pos].to_string()));
            sub_trie.subscribe(topic, test_subscriber).unwrap();
        }

        for lookup in lookups.iter() {
            // lookup topic "a" subscriber
            let parsed_lookup = parse_topic(lookup.lookup_topic.clone());
            let subs = sub_trie.lookup(&parsed_lookup).unwrap();
            assert_eq!(subs.len(), lookup.match_count);
            for id in lookup.match_ids.iter() {
                assert_eq!(id, &subs[*&id].id());
            }
        }
    }
}
