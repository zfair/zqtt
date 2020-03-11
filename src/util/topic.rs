use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref IDENT: Regex = Regex::new(r"[_0-9a-zA-Z]+").unwrap();
}

/// Subscription topic.
///
/// Some example topics:
///
/// - `a/b`
/// - `a/b/#`
/// - `a/+/b/#`
/// - `a/+/+/b/#`
///
/// ## Syntax
///
/// ```antlr
/// grammar topic;
///
/// IDENT : [_0-9a-zA-Z]+ ;
///
/// topic : channel ('/' '#')? EOF
///       | '#' EOF
///       ;
///
/// channel : IDENT ('/' channel)?
///         | '+' '/' channel
///         ;
/// ```
pub struct Topic {
    channels: Vec<TopicNode>,
}

/// Node of the subscription topic.
#[derive(Debug, Clone, PartialEq)]
enum TopicNode {
    /// A concrete channel name.
    Name(String),

    /// The `+` wildcard, matched between its parent and child topics.
    SingleWildcard,

    /// The `#` wildcard, only at the end of the topic string.
    MultipleWildcard,
}

impl Topic {
    /// Create a new subscription topic from a string.
    pub fn new(topic: &String) -> Result<Self, &'static str> {
        match TopicParser::new(topic) {
            Ok(mut parser) => parser.parse(),
            Err(e) => Err(e),
        }
    }
}

struct TopicParser {
    pos: usize,
    tokens: Vec<TopicNode>,
}

impl TopicParser {
    fn new(topic: &String) -> Result<Self, &'static str> {
        match Self::scan(topic) {
            Ok(tokens) => Ok(Self { pos: 0, tokens }),
            Err(e) => Err(e),
        }
    }

    /// Tokenize the subscription string into `TopicNode`s for further parsing.
    fn scan(topic: &String) -> Result<Vec<TopicNode>, &'static str> {
        let tokens = topic.split('/').map(move |tk| match tk {
            "+" => Some(TopicNode::SingleWildcard),
            "#" => Some(TopicNode::MultipleWildcard),
            _ if IDENT.is_match(tk) => Some(TopicNode::Name(tk.to_string())),
            _ => None,
        });

        let mut ret = vec![];
        for tk in tokens {
            match tk {
                None => return Err("Invalid characters in topic"),
                Some(tk) => ret.push(tk.to_owned()),
            }
        }

        Ok(ret)
    }

    fn parse(&mut self) -> Result<Topic, &'static str> {
        match self.parse_topic() {
            Ok(_) => Ok(Topic {
                channels: self.tokens.to_owned(),
            }),
            Err(e) => Err(e),
        }
    }

    fn parse_topic(&mut self) -> Result<(), &'static str> {
        match self.cur() {
            Some(TopicNode::MultipleWildcard) => {
                if let None = self.lookahead() {
                    return Ok(());
                }
                Err("Toplevel multi-level wildcard '#' should not have trailing")
            }
            Some(e) if e == &TopicNode::MultipleWildcard => {
                if let None = self.lookahead() {
                    return Ok(());
                }
                Err("Multi-level wildcard '#' should be placed at the end")
            }
            _ => self.parse_channel()
        }
    }

    fn parse_channel(&mut self) -> Result<(), &'static str> {
        loop {
            match self.cur() {
                Some(TopicNode::Name(_)) => {
                    if let None = self.lookahead() {
                        self.advance();
                        break;
                    }
                    self.advance();
                    continue;
                }
                Some(TopicNode::SingleWildcard) => {
                    if let None = self.lookahead() {
                        return Err("Single-level '+' cannot be placed at the end");
                    }
                    self.advance();
                    continue;
                }
                None => return Err("Invalid empty topic"),
                _ => break,
            }
        }
        Ok(())
    }

    fn cur(&self) -> Option<&TopicNode> {
        self.tokens.get(self.pos)
    }

    fn lookahead(&self) -> Option<&TopicNode> {
        self.tokens.get(self.pos + 1)
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::topic::Topic;

    #[test]
    fn it_parses_topic_without_sw() {
        let topic = Topic::new(&"a/b/c".to_string());
        assert!(topic.is_ok());
    }

    #[test]
    fn it_parses_topic_with_various_sw() {
        let topic = Topic::new(&"a/b/+/+/c".to_string());
        assert!(topic.is_ok());
    }

    #[test]
    fn it_parses_topic_with_toplevel_sw() {
        let topic = Topic::new(&"+/b/+/c".to_string());
        assert!(topic.is_ok());
    }

    #[test]
    fn it_parses_topic_with_mw() {
        let topic = Topic::new(&"+/b/+/c/#".to_string());
        assert!(topic.is_ok());
    }

    #[test]
    fn it_parses_topic_all() {
        let topic = Topic::new(&"#".to_string());
        assert!(topic.is_ok());
    }

    #[test]
    fn it_cannot_parse_empty_topic() {
        let topic = Topic::new(&"".to_string());
        assert!(topic.is_err());
    }

    #[test]
    fn it_cannot_parse_topic_with_toplevel_mw() {
        let topic = Topic::new(&"#/a".to_string());
        assert!(topic.is_err());
    }

    #[test]
    fn it_cannot_parse_topic_with_trailing_sw() {
        let topic = Topic::new(&"a/+".to_string());
        assert!(topic.is_err());
    }
}
