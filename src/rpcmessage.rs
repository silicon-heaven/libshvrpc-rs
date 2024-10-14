use crate::metamethod::AccessLevel;
use shvproto::{RpcValue, Value};
use shvproto::metamap::*;
// use std::collections::BTreeMap;
use shvproto::rpcvalue::{IMap, List};
// use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicI64, Ordering};
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde::de::Visitor;
use crate::rpcframe::RpcFrame;
use crate::rpctype;

static G_RPC_REQUEST_COUNT: AtomicI64 = AtomicI64::new(0);

pub type RqId = i64;
pub type PeerId = i64;

// backward compatibility
pub type CliId = PeerId;

pub enum Tag {
    RequestId = rpctype::Tag::USER as isize, // 8
    ShvPath = 9,
    Method = 10,
    CallerIds = 11,
    ProtocolType = 12, //needed when destination client is using different version than source one to translate raw message data to correct format
    RevCallerIds = 13, // SHV2 compatibility
    Access = 14,
    UserId = 16,
    AccessLevel = 17,
    SeqNo = 18,
    Source = 19,
    Repeat = 20,
    Part = 21,
    MAX
}
pub enum Key {Params = 1, Result, Error, ErrorCode, ErrorMessage, MAX }

#[derive(Clone, Debug)]
pub struct RpcMessage (RpcValue);
impl RpcMessage {
    pub fn meta(&self) -> &MetaMap {
        self.0.meta()
    }
    pub fn from_meta(meta: MetaMap) -> Self {
        RpcMessage(RpcValue::from(IMap::new()).set_meta(Some(meta)))
    }
    pub fn from_rpcvalue(rv: RpcValue) -> Result<Self, &'static str> {
        if rv.meta().is_empty() {
            return Err("Meta is empty.");
        }
        if rv.is_imap() {
            return Ok(Self(rv))
        }
        Err("Value must be IMap!")
    }
    pub fn as_rpcvalue(&self) -> &RpcValue {
        &self.0
    }
    pub fn to_cpon(&self) -> String {
        self.0.to_cpon()
    }
    pub fn to_chainpack(&self) -> Vec<u8> {
        self.0.to_chainpack()
    }
    pub fn next_request_id() -> RqId {
        let old_id = G_RPC_REQUEST_COUNT.fetch_add(1, Ordering::SeqCst);
        old_id + 1
    }
    pub fn to_frame(&self) -> crate::Result<RpcFrame> {
        RpcFrame::from_rpcmessage(self)
    }
    pub fn param(&self) -> Option<&RpcValue> { self.key(Key::Params as i32) }
    pub fn set_param(&mut self, rv: impl Into<RpcValue>) -> &mut Self  { self.set_param_opt(Some(rv.into())) }
    pub fn set_param_opt(&mut self, rv: Option<RpcValue>) -> &mut Self  { self.set_key(Key::Params, rv); self }
    pub fn result(&self) -> Result<&RpcValue, RpcError> {
        match self.key(Key::Result as i32) {
            None => {
                match self.error() {
                    None => {Err(RpcError{ code: RpcErrorCode::InternalError, message: "Neither 'result' nor 'error' key found in RPC response.".to_string() })}
                    Some(err) => {Err(err)}
                }
            }
            Some(rv) => {Ok(rv)}
        }
    }
    pub fn set_result(&mut self, rv: impl Into<RpcValue>) -> &mut Self { self.set_key(Key::Result, Some(rv.into())); self }
    pub fn set_result_or_error(&mut self, result: Result<RpcValue, RpcError>) -> &mut Self {
        match result {
            Ok(val) => { self.set_result(val) }
            Err(err) => { self.set_error(err) }
        };
        self
    }
    pub fn error(&self) -> Option<RpcError> {
        self.key(Key::Error as i32).and_then(RpcError::from_rpcvalue)
    }
    pub fn set_error(&mut self, err: RpcError) -> &mut Self {
        self.set_key(Key::Error, Some(err.to_rpcvalue()));
        self
    }
    pub fn is_success(&self) -> bool {
        self.result().is_ok()
    }
    pub fn is_error(&self) -> bool {
        self.error().is_some()
    }

    fn tag<Idx>(&self, key: Idx) -> Option<&RpcValue>
        where Idx: GetIndex
    {
        self.0.meta().get(key)
    }
    fn set_tag<Idx>(&mut self, key: Idx, rv: Option<RpcValue>) -> &mut Self
        where Idx: GetIndex
    {
        if let Some(mm) = self.0.meta_mut(){
            match rv {
                Some(rv) => { mm.insert(key, rv); },
                None => { mm.remove(key); },
            };
            self
        } else {
            panic!("Not RpcMessage")
        }
    }
    fn key(&self, key: i32) -> Option<&RpcValue> {
        if let Value::IMap(m) = self.0.value() {
            return m.get(&key);
        }
        None
    }
    fn set_key(&mut self, key: Key, rv: Option<RpcValue>) -> &mut Self {
        if let Value::IMap(m) = self.0.value_mut() {
            match rv {
                Some(rv) => m.insert(key as i32, rv),
                None => m.remove(&(key as i32)),
            };
            self
        } else {
            panic!("Not RpcMessage")
        }
    }
    pub fn new_request(shvpath: &str, method: &str, param: Option<RpcValue>) -> Self {
        Self::create_request_with_id(Self::next_request_id(), shvpath, method, param)
    }
    pub fn new_signal(shvpath: &str, method: &str, param: Option<RpcValue>) -> Self {
        let mut msg = Self::default();
        msg.set_shvpath(shvpath);
        msg.set_method(method);
        msg.set_param_opt(param);
        msg
    }
    pub fn new_signal_with_source(shvpath: &str, method: &str, source: &str, param: Option<RpcValue>) -> Self {
        let mut msg = Self::default();
        msg.set_shvpath(shvpath);
        msg.set_method(method);
        msg.set_source(source);
        msg.set_param_opt(param);
        msg
    }
    pub fn create_request_with_id(rq_id: RqId, shvpath: &str, method: &str, param: Option<RpcValue>) -> Self {
        let mut msg = Self::default();
        msg.set_request_id(rq_id);
        if !shvpath.is_empty() {
            msg.set_shvpath(shvpath);
        }
        msg.set_method(method);
        if let Some(rv) = param {
            msg.set_param(rv);
        }
        msg
    }
    pub fn prepare_response(&self) -> Result<Self, &'static str> {
        Self::prepare_response_from_meta(self.as_rpcvalue().meta())
    }
    pub fn prepare_response_from_meta(meta: &MetaMap) -> Result<Self, &'static str> {
        let meta = RpcFrame::prepare_response_meta(meta)?;
        Ok(Self::from_meta(meta))
    }
}
impl Default for RpcMessage {
    fn default() -> Self {
        let mut mm = MetaMap::new();
        mm.insert(rpctype::Tag::MetaTypeId as i32, RpcValue::from(rpctype::GlobalNS::MetaTypeID::ChainPackRpcMessage as i32));
        //mm.insert(Tag::Method as i32, RpcValue::from(method));
        RpcMessage(RpcValue::new(IMap::new().into(),Some(mm)))
    }
}

pub trait RpcMessageMetaTags {
    type Target;

    fn tag(&self, id: i32) -> Option<&RpcValue>;
    fn set_tag(&mut self, id: i32, val: Option<RpcValue>) -> &mut Self::Target;

    fn is_request(&self) -> bool {
        self.request_id().is_some() && self.method().is_some()
    }
    fn is_response(&self) -> bool {
        self.request_id().is_some() && self.method().is_none()
    }
    fn is_signal(&self) -> bool {
        self.request_id().is_none() && self.method().is_some()
    }

    fn request_id(&self) -> Option<RqId> {
        self.tag(Tag::RequestId as i32).map(|rv| rv.as_i64())
    }
    fn try_request_id(&self) -> crate::Result<RqId> {
        self.request_id().ok_or_else(|| "Request id not exists.".into())
    }
    fn set_request_id(&mut self, id: RqId) -> &mut Self::Target {
        self.set_tag(Tag::RequestId as i32, Some(RpcValue::from(id)))
    }
    fn shv_path(&self) -> Option<&str> {
        self.tag(Tag::ShvPath as i32).map(RpcValue::as_str)
    }
    fn set_shvpath(&mut self, shv_path: &str) -> &mut Self::Target {
        self.set_tag(Tag::ShvPath as i32, Some(RpcValue::from(shv_path)))
    }
    fn method(&self) -> Option<&str> {
        self.tag(Tag::Method as i32).map(RpcValue::as_str)
    }
    fn set_method(&mut self, method: &str) -> &mut Self::Target {
        self.set_tag(Tag::Method as i32, Some(RpcValue::from(method)))
    }
    fn source(&self) -> Option<&str> {
        self.tag(Tag::Source as i32).map(RpcValue::as_str)
    }
    fn set_source(&mut self, source: &str) -> &mut Self::Target {
        self.set_tag(Tag::Source as i32, Some(RpcValue::from(source)))
    }
    fn access_level(&self) -> Option<i32> {
        self.tag(Tag::AccessLevel as i32)
            .map(RpcValue::as_i32)
            .or_else(|| self.tag(Tag::Access as i32)
                     .map(RpcValue::as_str)
                     .and_then(|s| s.split(',')
                               .find_map(AccessLevel::from_str)
                               .map(|v| v as i32)))
    }
    fn set_access_level(&mut self, grant: AccessLevel) -> &mut Self::Target {
        self.set_tag(Tag::Access as i32, Some(RpcValue::from(grant.as_str())));
        self.set_tag(Tag::AccessLevel as i32, Some(RpcValue::from(grant as i32)))
    }

    fn caller_ids(&self) -> Vec<PeerId> {
        let t = self.tag(Tag::CallerIds as i32);
        match t {
            None => Vec::new(),
            Some(rv) => {
                if rv.is_int() {
                    return vec![rv.as_int() as PeerId];
                }
                if rv.is_list() {
                    return rv.as_list().iter().map(|v| v.as_int() as PeerId).collect();
                }
                vec![]
            },
        }
    }
    fn set_caller_ids(&mut self, ids: &[PeerId]) -> &mut Self::Target {
        if ids.is_empty() {
            return self.set_tag(Tag::CallerIds as i32, None);
        }
        if ids.len() == 1 {
            return self.set_tag(Tag::CallerIds as i32, Some(RpcValue::from(ids[0] as PeerId)));
        }
        let lst: List = ids.iter().map(|v| RpcValue::from(*v)).collect();
        return self.set_tag(Tag::CallerIds as i32, Some(RpcValue::from(lst)));
    }
    fn push_caller_id(&mut self, id: PeerId) -> &mut Self::Target {
        let mut ids = self.caller_ids();
        ids.push(id as PeerId);
        self.set_caller_ids(&ids)
    }
    fn pop_caller_id(&mut self) -> Option<PeerId> {
        let mut ids = self.caller_ids();
        let id = ids.pop();
        match id {
            Some(id) => {
                self.set_caller_ids(&ids);
                Some(id)
            }
            None => {
                None
            }
        }
    }
}
impl RpcMessageMetaTags for RpcMessage {
    type Target = Self;

    fn tag(&self, id: i32) -> Option<&RpcValue> {
        self.tag(id)
    }
    fn set_tag(&mut self, id: i32, val: Option<RpcValue>) -> &mut Self::Target {
        self.set_tag(id, val)
    }
}

impl RpcMessageMetaTags for MetaMap {
    type Target = MetaMap;

    fn tag(&self, id: i32) -> Option<&RpcValue> {
        self.get(id)
    }
    fn set_tag(&mut self, id: i32, val: Option<RpcValue>) -> &mut Self::Target {
        match val {
            Some(rv) => {
                self.insert(id, rv);
                self
            }
            None => {
                self.remove(id);
                self
            }
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RpcErrorCode {
    NoError = 0,
    InvalidRequest,	// The data sent is not a valid Request object.
    MethodNotFound,	// The method does not exist / is not available.
    InvalidParam,		// Invalid method parameter(s).
    InternalError,		// Internal RPC error.
    ParseError,		// Invalid JSON was received by the server. An error occurred on the server while parsing the JSON text.
    MethodCallTimeout,
    MethodCallCancelled,
    MethodCallException,
    PermissionDenied,
    Unknown,
    UserCode = 32
}
impl Display for RpcErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            RpcErrorCode::NoError => {"NoError"}
            RpcErrorCode::InvalidRequest => {"InvalidRequest"}
            RpcErrorCode::MethodNotFound => {"MethodNotFound"}
            RpcErrorCode::InvalidParam => {"InvalidParam"}
            RpcErrorCode::InternalError => {"InternalError"}
            RpcErrorCode::ParseError => {"ParseError"}
            RpcErrorCode::MethodCallTimeout => {"MethodCallTimeout"}
            RpcErrorCode::MethodCallCancelled => {"MethodCallCancelled"}
            RpcErrorCode::MethodCallException => {"MethodCallException"}
            RpcErrorCode::PermissionDenied => {"PermissionDenied"}
            RpcErrorCode::Unknown => {"Unknown"}
            RpcErrorCode::UserCode => {"UserCode"}
        };
        write!(f, "{}", s)
    }
}
impl TryFrom<i32> for RpcErrorCode {
    type Error = ();
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == RpcErrorCode::NoError as i32 => Ok(RpcErrorCode::NoError),
            x if x == RpcErrorCode::InvalidRequest as i32 => Ok(RpcErrorCode::InvalidRequest),	// The JSON sent is not a valid Request object.
            x if x == RpcErrorCode::MethodNotFound as i32 => Ok(RpcErrorCode::MethodNotFound),	// The method does not exist / is not available.
            x if x == RpcErrorCode::InvalidParam as i32 => Ok(RpcErrorCode::InvalidParam),		// Invalid method parameter(s).
            x if x == RpcErrorCode::InternalError as i32 => Ok(RpcErrorCode::InternalError),		// Internal JSON-RPC error.
            x if x == RpcErrorCode::ParseError as i32 => Ok(RpcErrorCode::ParseError),		// Invalid JSON was received by the server. An error occurred on the server while parsing the JSON text.
            x if x == RpcErrorCode::MethodCallTimeout as i32 => Ok(RpcErrorCode::MethodCallTimeout),
            x if x == RpcErrorCode::MethodCallCancelled as i32 => Ok(RpcErrorCode::MethodCallCancelled),
            x if x == RpcErrorCode::MethodCallException as i32 => Ok(RpcErrorCode::MethodCallException),
            x if x == RpcErrorCode::PermissionDenied as i32 => Ok(RpcErrorCode::PermissionDenied),
            x if x == RpcErrorCode::Unknown as i32 => Ok(RpcErrorCode::Unknown),
            _ => Err(()),
        }
    }
}

#[derive(Clone)]
pub struct RpcError {
    pub code: RpcErrorCode,
    pub message: String,
}

enum RpcErrorKey { Code = 1, Message }

impl RpcError {
    pub fn new(code: RpcErrorCode, msg: impl Into<String>) -> Self {
        RpcError {
            code,
            message: msg.into(),
        }
    }
    pub fn from_rpcvalue(rv: &RpcValue) -> Option<Self> {
        if rv.is_imap() {
            let m = rv.as_imap();
            let code = m.get(&(RpcErrorKey::Code as i32)).unwrap_or(&RpcValue::from(RpcErrorCode::Unknown as i32)).as_i32();
            let msg = if let Some(msg) = m.get(&(RpcErrorKey::Message as i32)) {
                msg.as_str().to_string()
            } else {
                "".to_string()
            };
            Some(RpcError {
                code: code.try_into().unwrap_or(RpcErrorCode::Unknown),
                message: msg,
            })
        } else {
            None
        }
    }
    pub fn to_rpcvalue(&self) -> RpcValue {
        let mut m = IMap::new();
        m.insert(RpcErrorKey::Code as i32, RpcValue::from(self.code as i32));
        m.insert(RpcErrorKey::Message as i32, RpcValue::from(&self.message));
        RpcValue::from(m)
    }
}
impl Default for RpcError {
    fn default() -> Self {
        RpcError {
            code: RpcErrorCode::NoError,
            message: "".to_string(),
        }
    }
}
impl Debug for RpcError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{code: {}, message: {}}}", self.code, self.message)
    }
}

impl Display for RpcError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} - {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {
}
impl fmt::Display for RpcMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_rpcvalue().to_cpon())
    }
}
/*
impl fmt::Debug for RpcMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_rpcvalue().to_cpon())
    }
}
*/
impl Serialize for RpcMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let bytes = self.to_chainpack();
        serializer.serialize_bytes(&bytes)
    }
}

struct RpcMessageVisitor;

impl<'de> Visitor<'de> for RpcMessageVisitor {
    type Value = RpcMessage;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("RpcMessage")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
    {
        match RpcValue::from_chainpack(value) {
            Ok(rv) => match RpcMessage::from_rpcvalue(rv) {
                Ok(msg) => Ok(msg),
                Err(err) => Err(E::custom(format!("RpcMessage create error: {}", err))),
            },
            Err(err) => Err(E::custom(format!("RpcValue parse error: {}", err))),
        }
    }
}

impl<'de> Deserialize<'de> for RpcMessage {
    fn deserialize<D>(deserializer: D) -> Result<RpcMessage, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(RpcMessageVisitor)
    }
}

#[cfg(test)]
mod test {
    use shvproto::RpcValue;
    use crate::RpcMessage;
    use crate::metamethod::AccessLevel;
    use crate::rpcmessage::RpcMessageMetaTags;
    use crate::rpcmessage::Tag;

    #[test]
    fn rpc_request() {
        let id = RpcMessage::next_request_id();
        let mut rq = RpcMessage::create_request_with_id(id, "foo/bar", "baz", None);
        let params = RpcValue::from(123);
        rq.set_param(params.clone());
        assert_eq!(rq.param(), Some(&params));
        let caller_ids = vec![1,2,3];
        rq.set_caller_ids(&caller_ids);
        assert_eq!(&rq.caller_ids(), &caller_ids);
        let id = rq.pop_caller_id();
        assert_eq!(id, Some(3));
        assert_eq!(rq.caller_ids(), vec![1,2]);
        let id = rq.pop_caller_id();
        assert_eq!(id, Some(2));
        let id = rq.pop_caller_id();
        assert_eq!(id, Some(1));
        let id = rq.pop_caller_id();
        assert_eq!(id, None);
        rq.push_caller_id(4);
        let mut resp = rq.prepare_response().unwrap();
        assert_eq!(&resp.caller_ids(), &vec![4]);
        assert_eq!(resp.pop_caller_id(), Some(4));
        //let cpon = rq.as_rpcvalue().to_cpon();
        //assert_eq!(cpon, format!("<1:1,8:{},10:\"foo\">i{{1:123}}", id + 1));
    }

    #[test]
    fn rpc_msg_access_level_none() {
        let rq = RpcMessage::new_request("foo/bar", "baz", None);
        assert_eq!(rq.access_level(), None);
    }

    #[test]
    fn rpc_msg_access_level_some() {
        let mut rq = RpcMessage::new_request("foo/bar", "baz", None);
        rq.set_access_level(AccessLevel::Read);
        assert_eq!(rq.access_level(), Some(AccessLevel::Read as i32));
    }

    #[test]
    fn rpc_msg_access_level_compat() {
        let mut rq = RpcMessage::new_request("foo/bar", "baz", None);
        rq.set_tag(Tag::Access as i32, Some(RpcValue::from("srv")));
        assert_eq!(rq.access_level(), Some(AccessLevel::Service as i32));
    }
}
