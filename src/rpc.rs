use std::fmt::{Display, Formatter};
use glob::Pattern;
use shvproto::RpcValue;
use shvproto::Map;

#[derive(Debug, Clone)]
pub struct Subscription {
    pub paths: String,
    pub methods: String,
}
impl Subscription {
    pub fn new(paths: &str, methods: &str) -> Self {
        let methods = if methods.is_empty() { "?*" } else { methods };
        Self {
            paths: paths.to_string(),
            methods: methods.to_string(),
        }
    }
    pub fn split(subscr: &str) -> (&str, &str) {
        match subscr.find(':') {
            None => { (subscr, "?*") }
            Some(ix) => {
                let methods = &subscr[ix+1 ..];
                let methods = if methods.is_empty() { "?*" } else { methods };
                (&subscr[0 .. ix], methods)
            }
        }
    }
    pub fn from_str_unchecked(subscription: &str) -> Self {
        let (paths, methods) = Self::split(subscription);
        Self::new(paths, methods)
    }
    pub fn from_rpcvalue(value: &RpcValue) -> Self {
        let m = value.as_map();
        let methods = m.get("method").unwrap_or(m.get("methods").unwrap_or_default()).as_str();
        let paths = m.get("path").unwrap_or(m.get("paths").unwrap_or_default()).as_str();
        Self::new(paths, methods)
    }

    pub fn to_rpcvalue(self) -> RpcValue {
        let mut m = Map::new();
        m.insert("paths".into(), self.paths.into());
        m.insert("methods".into(), self.methods.into());
        RpcValue::from(m)
    }
}
impl Display for Subscription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.paths, self.methods)
    }
}

#[derive(Debug, PartialEq)]
pub struct SubscriptionPattern {
    pub paths: Pattern,
    pub methods: Pattern,
}
impl SubscriptionPattern {
    pub fn new(paths: &str, methods: &str) -> crate::Result<Self> {
        // empty path matches SHV root, for example 'lschng' on root can signalize new service
        //let paths = if paths.is_empty() { "**" } else { paths };
        let methods = if methods.is_empty() { "?*" } else { methods };
        match Pattern::new(methods) {
            Ok(methods) => {
                match Pattern::new(paths) {
                    Ok(paths) => {
                        Ok(Self {
                            methods,
                            paths,
                        })
                    }
                    Err(err) => { Err(format!("{}", &err).into()) }
                }
            }
            Err(err) => { Err(format!("{}", &err).into()) }
        }
    }
    pub fn match_shv_method(&self, path: &str, method: &str) -> bool {
        self.paths.matches(path) && self.methods.matches(method)
    }
    pub fn from_rpcvalue(value: &RpcValue) -> crate::Result<Self> {
        let m = value.as_map();
        let methods = m.get("method").unwrap_or(m.get("methods").unwrap_or_default()).as_str();
        let paths = m.get("path").unwrap_or(m.get("paths").unwrap_or_default()).as_str();
        Self::new(paths, methods)
    }
    pub fn from_subscription(subscription: &Subscription) -> crate::Result<Self> {
        Self::new(&subscription.paths, &subscription.methods)
    }
    pub fn to_rpcvalue(&self) -> RpcValue {
        self.as_subscription().to_rpcvalue()
    }
    pub fn as_subscription(&self) -> Subscription {
        Subscription::new(self.paths.as_str(), self.methods.as_str())
    }
    //pub fn split_str(s: &str) -> (&str, &str) {
    //    let mut it = s.split(':');
    //    let paths = it.next().unwrap_or("");
    //    let methods = it.next().unwrap_or("");
    //    (paths, methods)
    //}
}
impl Display for SubscriptionPattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.paths.as_str(), self.methods.as_str())
    }
}
 impl From<SubscriptionPattern> for RpcValue {
     fn from(val: SubscriptionPattern) -> Self {
         let mut map = Map::new();
         map.insert("paths".to_string(), val.paths.as_str().into());
         map.insert("methods".to_string(), val.methods.as_str().into());
         RpcValue::from(map)
     }
 }
