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
        // if method is granted => signal is granted as well
        if self.path.matches(shv_ri.path()) && self.method.matches(shv_ri.method()) {
            // path and method match
            if let Some(glob_signal_pattern) = &self.signal {
                // glob has signal defined
                if let Some(ri_signal) = shv_ri.signal() {
                    // RI has signal defined
                    return glob_signal_pattern.matches(ri_signal);
                } else {
                    return false;
                }
            } else {
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
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
        let method = self.method();
        let signal = self.signal();
        Ok(Glob {
            path: Pattern::new(path)
                .map_err(|e| format!("Parse path glob: '{}' error: {}", self, e))?,
            method: Pattern::new(method)
                .map_err(|e| format!("Parse method glob: '{}' error: {}", self, e))?,
            signal: if let Some(signal) = signal {
                Some(
                    Pattern::new(signal)
                        .map_err(|e| format!("Parse signal glob: '{}' error: {}", self, e))?,
                )
            } else {
                None
            },
            ri: ShvRI::from_path_method_signal(path, method, signal)?,
        })
    }
    pub fn as_str(&self) -> &str {
        &self.ri
    }
    pub fn from_path_method_signal(path: &str, method: &str, signal: Option<&str>) -> Result<Self, String> {
        let ri = if let Some(signal) = signal {
            let method = if method.is_empty() { "*" } else { method };
            format!("{path}:{method}:{signal}")
        } else {
            format!("{path}:{method}")
        };
        Ok(Self::try_from(ri)?)
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
        let ri = ShvRI {
            ri: s,
            method_sep_ix,
            signal_sep_ix,
        };
        if ri.method().is_empty() {
            Err("Method must not be empty.")
        } else if ri.signal().is_some() && ri.signal().unwrap().is_empty() {
            Err("Signal, if present, must not be empty.")
        }
         else {
            Ok(ri)
        }
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
    pub ttl: Option<u32>,
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
                    ri: ShvRI::from_path_method_signal(paths, source, signal)?,
                    ttl: None,
                })
            }
        } else if value.is_list() {
            let lst = value.as_list();
            let ri = lst.first().map(|v| v.as_str()).unwrap_or_default();
            if ri.is_empty() {
                Err("Empty SHV RI".into())
            } else {
                let ttl = lst.get(1).unwrap_or_default().clone();
                Ok(SubscriptionParam {
                    ri: ri.try_into()?,
                    ttl: if ttl.is_null() { None } else { Some(ttl.as_u32()) },
                })
            }
        } else if value.is_string() {
            let ri = value.as_str();
            if ri.is_empty() {
                Err("Empty SHV RI".into())
            } else {
                Ok(SubscriptionParam {
                    ri: ri.try_into()?,
                    ttl: None,
                })
            }        } else {
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

#[cfg(test)]
mod tests {
    use crate::rpc::{ShvRI, SubscriptionParam};

    #[test]
    fn test_shvri() -> Result<(), String> {
        for (ri, path, method, signal, glob) in vec![
            (":*:*", "", "*", Some("*"), ":*:*"),
            ("some/path:method:signal", "some/path", "method", Some("signal"), "some/path:method:signal",),
            (":*", "", "*", None, ":*"),
            ("**:*:*", "**", "*", Some("*"), "**:*:*"),
        ] {
            let ri = ShvRI::try_from(ri)?;
            assert_eq!(
                (ri.path(), ri.method(), ri.signal(), ri.to_glob()?.as_str()),
                (path, method, signal, glob)
            );
        }
        Ok(())
    }
    #[test]
    #[should_panic]
    fn test_invalid_shvri() {
        ShvRI::try_from("::").unwrap();
    }
    #[test]
    fn test_glob() -> Result<(), String> {
        for (path, ri, is_match) in vec![
            (".app:name", "**:*", true),
            (".app:name", "**:get", false),
            (".app:name", "test:*", false),
            (".app:name", "test/**:get:*chng", false),

            ("sub/device/track:get", "**:*", true),
            ("sub/device/track:get", "**:get", true),
            ("sub/device/track:get", "test:*", false),
            ("sub/device/track:get", "test/**:get:*chng", false),

            ("test/device/track:get", "**:*", true),
            ("test/device/track:get", "**:get", true),
            ("test/device/track:get", "test/**:*", true),
            ("test/device/track:get", "test/**:get:*chng", false),

            ("test/device/track:get:chng", "**:*:*", true),
            ("test/device/track:get:chng", "**:get:*", true),
            ("test/device/track:get:chng", "test/**:get:*chng", true),
            ("test/device/track:get:chng", "test/*:ls:lsmod", false),
            ("test/device/track:get:chng", "test/**:get", true),

            ("test/device/track:get:mod", "**:*:*", true),
            ("test/device/track:get:mod", "**:get:*", true),
            ("test/device/track:get:mod", "test/**:get:*chng", false),
            ("test/device/track:get:mod", "test/*:ls:lsmod", false),
            ("test/device/track:get:mod", "test/**:get", true),

            ("test/device/track:ls:lsmod", "**:*:*", true),
            ("test/device/track:ls:lsmod", "**:get:*", false),
            ("test/device/track:ls:lsmod", "test/**:get:*chng", false),
            ("test/device/track:ls:lsmod", "test/*:ls:lsmod", true),
            ("test/device/track:ls:lsmod", "test/**:get", false),
        ] {
            // println!("{path} {ri}");
            let glob = ShvRI::try_from(ri)?.to_glob()?;
            let m = glob.match_shv_ri(&ShvRI::try_from(path)?);
            assert_eq!(m, is_match);
        }
        Ok(())
    }
    #[test]
    fn test_subscription_param() -> Result<(), String> {
        for sp1 in vec![
            SubscriptionParam { ri: ShvRI::try_from("*:*:*")?, ttl: None },
            SubscriptionParam { ri: ShvRI::try_from("*:*:chng")?, ttl: Some(0) },
            SubscriptionParam { ri: ShvRI::try_from("*:*:fchng")?, ttl: Some(123) },
        ] {
            let rv = sp1.to_rpcvalue();
            let sp2 = SubscriptionParam::from_rpcvalue(&rv)?;
            assert_eq!(sp1, sp2);
        }
        Ok(())
    }
}
