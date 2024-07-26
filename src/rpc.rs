use glob::Pattern;
use serde::{Deserialize, Serialize};
use shvproto::RpcValue;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Glob {
    path: Pattern,
    method: Pattern,
    signal: Option<Pattern>,
    ri: ShvRI,
}

impl Glob {
    pub fn match_shv_ri(&self, shv_ri: &ShvRI) -> bool {
        // if signal is granted => method is granted as well
        // path:: => path:
        // path:method: => path:method
        // path::signal => broker - doesn't know which method has the 'signal' => no implicit rule for method
        if self.path.matches(shv_ri.path()) && self.method.matches(shv_ri.path()) {
            if let Some(signal_pattern) = &self.signal {
                if let Some(signal) = shv_ri.signal() {
                    return signal_pattern.matches(signal);
                } else {
                    let any_signal = signal_pattern.as_str() == "*";
                    return any_signal;
                }
            } else if shv_ri.signal().is_none() {
                return true;
            }
        }
        false
    }
    pub fn as_str(&self) -> &str {
        self.ri.as_str()
    }
    pub fn path_str(&self) -> &str {
        self.path.as_str()
    }
    pub fn method_str(&self) -> &str {
        self.method.as_str()
    }
    pub fn signal_str(&self) -> Option<&str> {
        self.signal.as_ref().map(|s| s.as_str())
    }
}
impl TryFrom<&str> for Glob {
    type Error = String;
    fn try_from(s: &str) -> std::result::Result<Self, <Self as TryFrom<&str>>::Error> {
        let ri = ShvRI::try_from(s)?;
        ri.to_glob()
    }
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ShvRI {
    ri: String,
    method_sep_ix: usize,
    signal_sep_ix: Option<usize>,
}

impl ShvRI {
    pub fn path(&self) -> &str {
        &self.ri[0..self.method_sep_ix]
    }
    pub fn method(&self) -> &str {
        if let Some(ix) = self.signal_sep_ix {
            &self.ri[self.method_sep_ix + 1..ix]
        } else {
            &self.ri[self.method_sep_ix + 1..]
        }
    }
    pub fn signal(&self) -> Option<&str> {
        if let Some(ix) = self.signal_sep_ix {
            Some(&self.ri[ix + 1..])
        } else {
            None
        }
    }
    pub fn has_signal(&self) -> bool {
        self.signal_sep_ix.is_some()
    }
    pub fn to_glob(&self) -> Result<Glob, String> {
        let path = self.path();
        let path = if path.is_empty() { "**" } else { path };
        let method = self.method();
        let method = if method.is_empty() { "*" } else { method };
        let signal = self.signal().map(|s| if s.is_empty() { "*" } else { s });
        let signal = if let Some(signal) = signal {
            Some(
                Pattern::new(signal)
                    .map_err(|e| format!("Parse signal glob: '{}' error: {}", self, e))?,
            )
        } else {
            None
        };
        Ok(Glob {
            path: Pattern::new(path)
                .map_err(|e| format!("Parse path glob: '{}' error: {}", self, e))?,
            method: Pattern::new(method)
                .map_err(|e| format!("Parse method glob: '{}' error: {}", self, e))?,
            signal,
            ri: self.clone(),
        })
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
    pub fn normalized(&self) -> Self {
        let path = if self.path().is_empty() {
            "**"
        } else {
            self.path()
        };
        let method = if self.method().is_empty() {
            "**"
        } else {
            self.method()
        };
        let signal = self.signal().map(|s| if s.is_empty() { "* " } else { s });
        Self::from_path_method_signal(path, method, signal)
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
            return Err("Method separtor ':' is missing.");
        };
        let signal_sep_ix = s[method_sep_ix + 1..]
            .find(':')
            .map(|ix| ix + method_sep_ix + 1);
        Ok(ShvRI {
            ri: s,
            method_sep_ix,
            signal_sep_ix,
        })
    }
}
impl Display for ShvRI {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.as_str())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionParam {
    pub ri: ShvRI,
    pub ttl: u32,
}
impl SubscriptionParam {
    pub fn from_rpcvalue(value: &RpcValue) -> Result<Self, String> {
        if value.is_map() {
            let m = value.as_map();
            let paths = m
                .get("paths")
                .unwrap_or(m.get("path").unwrap_or_default())
                .as_str();
            let source = m.get("source").unwrap_or_default().as_str();
            let signal = m
                .get("signal")
                .or(m.get("methods"))
                .or(m.get("method"))
                .map(|v| v.as_str());
            if paths.is_empty() && source.is_empty() && signal.is_none() {
                Err("Empty map".into())
            } else {
                Ok(SubscriptionParam {
                    ri: ShvRI::from_path_method_signal(paths, source, signal),
                    ttl: 0,
                })
            }
        } else if value.is_list() {
            let lst = value.as_list();
            let ri = lst.first().map(|v| v.as_str()).unwrap_or_default();
            if ri.is_empty() {
                Err("Empty SHV RI".into())
            } else {
                let ttl = lst.get(1).map(|v| v.as_u32()).unwrap_or(0);
                Ok(SubscriptionParam {
                    ri: ri.try_into()?,
                    ttl,
                })
            }
        } else {
            Err("Unsupported RPC value type.".into())
        }
    }
    pub fn to_rpcvalue(&self) -> RpcValue {
        let lst = vec![
            RpcValue::from(self.ri.to_string()),
            RpcValue::from(self.ttl),
        ];
        RpcValue::from(lst)
    }
}

#[derive(Debug)]
pub struct Subscription {
    pub param: SubscriptionParam,
    pub glob: Glob,
}

impl Subscription {
    pub fn new(subpar: &SubscriptionParam) -> crate::Result<Self> {
        let glob = subpar.ri.to_glob()?;
        Ok(Self {
            param: subpar.clone(),
            glob,
        })
    }
    pub fn match_shv_ri(&self, shv_ri: &ShvRI) -> bool {
        self.glob.match_shv_ri(shv_ri)
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc::ShvRI;

    #[test]
    fn test_shvri() -> Result<(), String> {
        for (ri, path, method, signal, glob) in vec![
            ("::", "", "", Some(""), "**:*:*"),
            (
                "some/path:method:signal",
                "some/path",
                "method",
                Some("signal"),
                "some/path:method:signal",
            ),
            (":", "", "", None, "**:*"),
            ("**::", "**", "", Some(""), "**:*:*"),
        ] {
            let ri = ShvRI::try_from(ri)?;
            assert_eq!(
                (ri.path(), ri.method(), ri.signal(), ri.as_str()),
                (path, method, signal, glob)
            );
        }
        Ok(())
    }
}
