use std::fmt::{Display, Formatter};
use glob::Pattern;
use shvproto::RpcValue;
use shvproto::Map;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ShvRI {
    ri: String,
    method_sep_ix: usize,
    signal_sep_ix: Option<usize>,
}

impl ShvRI {
    pub fn path(&self) -> &str {
        &self.ri[0 .. self.method_sep_ix]
    }
    pub fn method(&self) -> &str {
        if let Some(ix) = self.signal_sep_ix {
            &self.ri[self.method_sep_ix + 1 .. ix]
        } else {
            &self.ri[self.method_sep_ix + 1 ..]
        }
    }
    pub fn signal(&self) -> Option<&str> {
        if let Some(ix) = self.signal_sep_ix {
            Some(&self.ri[ix + 1 ..])
        } else {
            None
        }
    }
    pub fn has_signal(&self) -> bool {
        self.signal_sep_ix.is_some()
    }
    pub fn to_glob(&self) -> String {
        let path = self.path();
        let method = self.method();
        if let Some(signal) = self.signal() {
            format!("{}:{}:{}",
                    if path.is_empty() {"**"} else {path},
                    if method.is_empty() {"*"} else {method},
                    if signal.is_empty() {"*"} else {signal}
            )
        } else {
            format!("{}:{}",
                    if path.is_empty() {"**"} else {path},
                    if method.is_empty() {"*"} else {method},
            )
        }
    }
    pub fn as_str(&self) -> &str {
        &self.ri
    }
    pub fn from_path_method_signal(path: &str, method: &str, signal: Option<&str>) -> Self {
        let ri = if let Some(signal) = signal {
            format!("{path}:{method}:{signal}")
        } else {
            format!("{path}:{method}")
        };
        Self::try_from(ri).expect("Valid RI string")
    }
}
impl TryFrom<&str> for ShvRI {
    type Error = &'static str;
    fn try_from(s: &str) -> std::result::Result<Self, <Self as TryFrom<&str>>::Error> {
        let ri = s.to_owned();
        Self::try_from(ri)
    }
}
impl TryFrom<String> for ShvRI {
    type Error = &'static str;
    fn try_from(s: String) -> std::result::Result<Self, <Self as TryFrom<String>>::Error> {
        let Some(method_sep_ix) = s[..].find(':') else {
            return Err("Method separtor ':' is missing.")
        };
        let signal_sep_ix = s[method_sep_ix + 1 ..].find(':');
        Ok(ShvRI {
            ri: s,
            method_sep_ix,
            signal_sep_ix,
        })
    }
}
#[derive(Debug, Clone)]
pub struct Subscription {
    pub ri: ShvRI,
    pub ttl: u32,
}
impl Subscription {
    pub fn from_rpcvalue(value: &RpcValue) -> Result<Self, String> {
        if value.is_map() {
            let m = value.as_map();
            let paths = m.get("paths").unwrap_or(m.get("path").unwrap_or_default()).as_str();
            let source = m.get("source").unwrap_or_default().as_str();
            let signal = m.get("signal").or(m.get("methods")).or(m.get("method"))
                                                .map(|v| v.as_str());
            if paths.is_empty() && source.is_empty() && signal.is_none() {
                Err("Empty map".into())
            } else {
                Ok(Subscription {
                    ri: ShvRI::from_path_method_signal(paths, source, signal),
                    ttl: 0,
                })
            }
        }
        else if value.is_list() {
            let lst = value.as_list();
            let ri = lst.get(0).map(|v| v.as_str()).unwrap_or_default();
            if ri.is_empty() {
                Err("Empty SHV RI".into())
            } else {
                let ttl = lst.get(1).map(|v| v.as_u32()).unwrap_or(0);
                Ok(Subscription {
                    ri: ri.try_into()?,
                    ttl,
                })
            }
        } else {
            Err("Unsupported RPC value type.".into())
        }
    }
    pub fn to_rpcvalue(&self) -> RpcValue {
        let mut m = Map::new();
        m.insert("signalRI".into(), self.ri.as_str().into());
        if self.ttl > 0 {
            m.insert("ttl".into(), self.ttl.into());
        }
        RpcValue::from(m)
    }
}

#[derive(Debug)]
pub struct SubscriptionPattern(Pattern);

impl SubscriptionPattern {
    pub fn new(shv_ri: &ShvRI) -> crate::Result<Self> {
        match Pattern::new(&shv_ri.to_glob()) {
            Ok(patt) => {
                Ok(Self(patt))
            }
            Err(err) => { Err(format!("Signal RI pattern error: {}", &err).into()) }
        }
    }
    pub fn match_shv_ri(&self, shv_ri: &ShvRI) -> bool {
        self.0.matches(shv_ri.as_str())
    }
    pub fn from_subscription(subscription: &Subscription) -> crate::Result<Self> {
        Self::new(&subscription.ri)
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
impl Display for SubscriptionPattern {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}