use std::fmt::{Display, Formatter};
use glob::Pattern;
use shvproto::RpcValue;
use shvproto::Map;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Subscription {
    pub paths: String,
    pub signal: String,
    pub source: String,
}
impl Subscription {
    pub fn new(paths: &str, signal: &str, source: &str) -> Self {
        let paths = if paths.is_empty() { "**" } else { paths };
        let signal = if signal.is_empty() { "*" } else { signal };
        let source = if source.is_empty() { "*" } else { source };
        Self {
            paths: paths.to_string(),
            signal: signal.to_string(),
            source: source.to_string(),
        }
    }
    pub fn from_rpcvalue(value: &RpcValue) -> Self {
        let m = value.as_map();
        let paths = m.get("paths").unwrap_or(m.get("path").unwrap_or_default()).as_str();
        let signal = m.get("signal").unwrap_or(m.get("methods").unwrap_or(m.get("method").unwrap_or_default())).as_str();
        let source = m.get("source").unwrap_or_default().as_str();
        Self::new(paths, signal, source)
    }
    pub fn to_rpcvalue(self) -> RpcValue {
        let mut m = Map::new();
        m.insert("paths".into(), self.paths.into());
        m.insert("signal".into(), self.signal.into());
        m.insert("source".into(), self.source.into());
        RpcValue::from(m)
    }
}
impl Display for Subscription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = format!("{}:{}", self.paths, self.signal);
        if !(self.source.is_empty() || self.source == "*") {
            s = format!("{}:{}", &s, self.source);
        }
        write!(f, "{}", s)
    }
}

impl From<&SubscriptionPattern> for Subscription {
    fn from(p: &SubscriptionPattern) -> Self {
        Subscription {
            paths: p.paths.as_str().to_string(),
            signal: p.signal.as_str().to_string(),
            source: p.source.as_str().to_string(),
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct SubscriptionPattern {
    pub paths: Pattern,
    pub signal: Pattern,
    pub source: Pattern,
}
impl SubscriptionPattern {
    pub fn new(paths: &str, signal: &str, source: &str) -> crate::Result<Self> {
        let paths = if paths.is_empty() { "**" } else { paths };
        let signal = if signal.is_empty() { "*" } else { signal };
        let source = if source.is_empty() { "*" } else { source };
        match Pattern::new(paths) {
            Ok(paths) => {
                match Pattern::new(signal) {
                    Ok(signal) => {
                        match Pattern::new(source) {
                            Ok(source) => {
                                Ok(Self {
                                    paths,
                                    signal,
                                    source,
                                })
                            }
                            Err(err) => { Err(format!("Source pattern error: {}", &err).into()) }
                        }
                    }
                    Err(err) => { Err(format!("Signal pattern error: {}", &err).into()) }
                }
            }
            Err(err) => { Err(format!("Paths pattern error: {}", &err).into()) }
        }
    }
    pub fn match_shv_method(&self, paths: &str, signal: &str, source: &str) -> bool {
        self.paths.matches(paths) && self.signal.matches(signal) && self.source.matches(source)
    }
    pub fn from_rpcvalue(value: &RpcValue) -> crate::Result<Self> {
        Self::from_subscription(&Subscription::from_rpcvalue(value))
    }
    pub fn from_subscription(subscription: &Subscription) -> crate::Result<Self> {
        Self::new(&subscription.paths, &subscription.signal, &subscription.source)
    }
    pub fn to_rpcvalue(&self) -> RpcValue {
        self.to_subscription().to_rpcvalue()
    }
    pub fn to_subscription(&self) -> Subscription {
        Subscription::new(self.paths.as_str(), self.signal.as_str(), self.source.as_str())
    }
}
impl Display for SubscriptionPattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = format!("{}:{}", self.paths.as_str(), self.signal.as_str());
        if !(self.source.as_str().is_empty() || self.source.as_str() == "*") {
            s = format!("{}:{}", &s, self.source.as_str());
        }
        write!(f, "{}", s)
    }
}
