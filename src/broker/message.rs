use actix::prelude::*;
use bytes::Bytes;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::ptr::NonNull;
use std::rc::Rc;

use crate::util;

/// Channel ID, namely the hash of a topic channel.
type ChanID = u64;

/// Subscription ID, a list of [ChanID]s.
type SSID = [ChanID];

lazy_static! {
    static ref SINGLE_WILDCARD: ChanID = util::hash_str("+");
    static ref MULTI_WILDCARD: ChanID = util::hash_str("#");
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubscribeError {
    SSIDNotFound,
    SubscriberNotFound,
}

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

impl Node {
    unsafe fn orphans(&mut self) {
        match self.parent {
            None => {}
            Some(parent) => {
                let parent_node = &mut *parent.as_ptr();
                // if this node has parent, it's chan_id must not None
                let this_node = parent_node.children.remove(&self.chan_id.unwrap()).unwrap();
                // make NonNull into Box, auto drop it
                Box::from_raw(this_node.unwrap().as_ptr());
                // check is parent
                if parent_node.subs.len() == 0 && parent_node.children.len() == 0 {
                    // remove orphans recursive
                    parent_node.orphans();
                }
            }
        }
    }
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
    pub fn new() -> Self {
        let boxed_node = Box::new(Node::default());
        SubTrie {
            root: Some(Box::into_raw_non_null(boxed_node)),
        }
    }

    pub fn subscribe(
        &mut self,
        ssid: &SSID,
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
                .insert(subscriber.id(), subscriber.to_owned());
        }
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        ssid: &SSID,
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
                        None => return Err(SubscribeError::SSIDNotFound),
                    }
                }
                None => unreachable!(),
            }
        }

        let match_node;
        unsafe {
            match_node = &mut *cur.unwrap().as_ptr();
        }

        // Remove subscriber rc
        // we should return an error if subscriber not found?
        let remove_result = match_node.subs.remove(&subscriber.id());
        if remove_result.is_none() {
            return Err(SubscribeError::SubscriberNotFound);
        }

        // Remove orphans
        if match_node.subs.len() == 0 && match_node.children.len() == 0 {
            unsafe {
                match_node.orphans();
            }
        }
        Ok(())
    }

    pub fn lookup(&self, ssid: &Vec<u64>) -> Result<Subscribers, SubscribeError> {
        let mut subs = Subscribers::new();
        self.do_lookup(&self.root, ssid.as_slice(), &mut subs)?;
        Ok(subs)
    }

    fn do_lookup(
        &self,
        node: &Option<NonNull<Node>>,
        ssid: &SSID,
        subs: &mut Subscribers,
    ) -> Result<(), SubscribeError> {
        if ssid.len() == 0 {
            // find match node
            unsafe {
                let this_node = &*node.unwrap().as_ptr();
                subs.extend(this_node.subs.to_owned());
                // search MULTI_WILDCARD at last level
                if let Some(mwcn) = this_node.children.get(&MULTI_WILDCARD) {
                    let mw_node = &(*mwcn.unwrap().as_ptr()).subs;
                    subs.extend(mw_node.to_owned());
                }
            }
            return Ok(());
        }

        let this_node;
        unsafe {
            this_node = &*node.unwrap().as_ptr();
        }

        if let Some(mwcn) = this_node.children.get(&MULTI_WILDCARD) {
            unsafe {
                let mw_node = &(*mwcn.unwrap().as_ptr()).subs;
                subs.extend(mw_node.to_owned());
            }
        }
        // dfs lookup single wildcard
        if let Some(swcn) = this_node.children.get(&SINGLE_WILDCARD) {
            self.do_lookup(swcn, &ssid[1..ssid.len()], subs)?;
        }
        if let Some(matchn) = this_node.children.get(&ssid[0]) {
            self.do_lookup(matchn, &ssid[1..ssid.len()], subs)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

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
        fn send(&self, _m: &super::Message) -> Result<(), SubscribeError> {
            Ok(())
        }
    }

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
            },
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
            .to_owned()
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
            let parsed_lookup = parse_topic(lookup.lookup_topic.to_owned());
            let subs = sub_trie.lookup(&parsed_lookup).unwrap();
            assert_eq!(subs.len(), lookup.match_count);
            for id in lookup.match_ids.iter() {
                assert_eq!(id, &subs[*&id].id());
            }
        }
    }

    struct UnSubscribeTestCase {
        unsubscribe_topic: String,
        unsubscribe_result: Result<(), SubscribeError>,
        lookup_topic: String,
        match_count: usize,
        match_ids: Vec<String>,
    }

    // add unsubscribe method test
    #[test]
    fn test_unsubscribe() {
        // use same topics with test_lookup function
        // because test_lookup guarantee those topics can insert into sub trie correctly
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

        let parsed_topics: Vec<Vec<u64>> = topics
            .to_owned()
            .into_iter()
            .map(|x| parse_topic(x.to_string()))
            .collect();
        let mut sub_trie = SubTrie::new();
        for (pos, topic) in parsed_topics.iter().enumerate() {
            // use topic name as subscriber id
            let test_subscriber = Rc::new(TestSubscriber::new(topics[pos].to_string()));
            sub_trie.subscribe(topic, test_subscriber).unwrap();
        }

        let test_cases: Vec<UnSubscribeTestCase> = vec![
            UnSubscribeTestCase {
                unsubscribe_topic: "#".to_string(),
                unsubscribe_result: Ok(()),
                lookup_topic: "a".to_string(),
                match_count: 1,
                match_ids: vec!["+".to_string()],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "#".to_string(),
                unsubscribe_result: Err(SubscribeError::SSIDNotFound),
                lookup_topic: "a".to_string(),
                match_count: 1,
                match_ids: vec!["+".to_string()],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "+".to_string(),
                unsubscribe_result: Ok(()),
                lookup_topic: "a".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "#".to_string(),
                // because “#” subscriber is an orphan
                unsubscribe_result: Err(SubscribeError::SSIDNotFound),
                lookup_topic: "a/b".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "#".to_string(),
                unsubscribe_result: Err(SubscribeError::SSIDNotFound),
                lookup_topic: "hello/world".to_string(),
                match_count: 2,
                match_ids: vec!["hello/#".to_string(), "hello/+".to_string()],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/#".to_string(),
                unsubscribe_result: Ok(()),
                lookup_topic: "hello/world".to_string(),
                match_count: 1,
                match_ids: vec!["hello/+".to_string()],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/+".to_string(),
                unsubscribe_result: Ok(()),
                lookup_topic: "hello/world".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/+".to_string(),
                unsubscribe_result: Err(SubscribeError::SubscriberNotFound),
                lookup_topic: "hello/world".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/+".to_string(),
                unsubscribe_result: Err(SubscribeError::SubscriberNotFound),
                lookup_topic: "hello/world/zqtt".to_string(),
                match_count: 1,
                match_ids: vec!["hello/+/zqtt".to_string()],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/+/zqtt".to_string(),
                unsubscribe_result: Ok(()),
                lookup_topic: "hello/world/zqtt".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            // after remove "hello/+/zqtt", node "hello/+" becomes an orphan
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/+".to_string(),
                // it's err change into SSIDNotFound
                unsubscribe_result: Err(SubscribeError::SSIDNotFound),
                lookup_topic: "hello/world/zqtt".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/+".to_string(),
                // it's err change into SSIDNotFound
                unsubscribe_result: Err(SubscribeError::SSIDNotFound),
                lookup_topic: "hello/world/zqtt".to_string(),
                match_count: 0,
                match_ids: vec![],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/mqtt/#".to_string(),
                // it's err change into SSIDNotFound
                unsubscribe_result: Ok(()),
                lookup_topic: "hello/mqtt/ohh".to_string(),
                match_count: 1,
                match_ids: vec!["hello/mqtt/+".to_string()],
            },
            UnSubscribeTestCase {
                unsubscribe_topic: "hello/mqtt/#".to_string(),
                unsubscribe_result: Err(SubscribeError::SSIDNotFound),
                lookup_topic: "hello/mqtt/bili/acfun".to_string(),
                match_count: 1,
                match_ids: vec!["hello/mqtt/+/+".to_string()],
            },
        ];

        let parsed_topics: Vec<Vec<u64>> = topics
            .to_owned()
            .into_iter()
            .map(|x| parse_topic(x.to_string()))
            .collect();
        let mut sub_trie = SubTrie::new();

        for (pos, topic) in parsed_topics.iter().enumerate() {
            // use topic name as subscriber id
            let test_subscriber = Rc::new(TestSubscriber::new(topics[pos].to_string()));
            sub_trie.subscribe(topic, test_subscriber).unwrap();
        }

        for case in test_cases.iter() {
            let parsed_unsubscribe = parse_topic(case.unsubscribe_topic.to_owned());
            let unsubscriber = Rc::new(TestSubscriber::new(case.unsubscribe_topic.to_string()));
            let unsubscribe_result = sub_trie.unsubscribe(&parsed_unsubscribe, unsubscriber);
            match unsubscribe_result {
                Ok(r) => {
                    assert_eq!(r, case.unsubscribe_result.unwrap());
                }
                Err(e) => {
                    assert_eq!(e, case.unsubscribe_result.unwrap_err());
                }
            }
            // lookup topic subscriber after unsubscribe
            let parsed_lookup = parse_topic(case.lookup_topic.to_owned());
            let subs = sub_trie.lookup(&parsed_lookup).unwrap();
            assert_eq!(subs.len(), case.match_count);
            for id in case.match_ids.iter() {
                assert_eq!(id, &subs[*&id].id());
            }
        }
    }
}
