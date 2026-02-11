use crate::metamethod::AccessLevel;
use shvproto::{RpcValue, Value};
use shvproto::metamap::{MetaMap, GetIndex};
use shvproto::rpcvalue::{IMap, List};
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
pub type SeqNo = u64;

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
pub enum Key {
    Params = 1,
    Result,
    Error,
    Delay,
    Abort,
}

pub enum AbortParam {
    Query,
    Abort,
}

impl From<AbortParam> for RpcValue {
    fn from(value: AbortParam) -> Self {
        match value {
            AbortParam::Query => false,
            AbortParam::Abort => true,
        }
        .into()
    }
}

impl TryFrom<&RpcValue> for AbortParam {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        if let Value::Bool(val) = value.value {
            Ok(if val { Self:: Abort } else { Self::Query })
        } else {
            Err(format!("Cannot parse AbortParam, expected type Bool, got: {}", value.type_name()))
        }
    }
}

static STATIC_EMPTY_METAMAP: std::sync::OnceLock<MetaMap> = std::sync::OnceLock::new();
static STATIC_NULL_RPCVALUE: std::sync::OnceLock<RpcValue> = std::sync::OnceLock::new();

#[derive(Clone,Debug)]
pub enum Response<'a> {
    Success(&'a RpcValue),
    Delay(f64),
}

impl<'a> Response<'a> {
    pub fn success(&self) -> Option<&'a RpcValue> {
        match self {
            Response::Success(rpc_value) => Some(rpc_value),
            _ => None,
        }
    }
    pub fn delay(&self) -> Option<f64> {
        match self {
            Response::Delay(progress) => Some(*progress),
            _ => None,
        }
    }
}


#[derive(Clone, Debug)]
pub struct RpcMessage (RpcValue);
impl RpcMessage {
    pub fn from_meta(meta: MetaMap) -> Self {
        RpcMessage(RpcValue::new(IMap::new().into(), Some(meta)))
    }
    pub fn from_rpcvalue(rv: RpcValue) -> Result<Self, &'static str> {
        if rv.meta.is_none() {
            return Err("Meta is empty.");
        }
        // FIXME: Include a check for mandatory meta keys as defined in SHV specification
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
    pub fn param(&self) -> Option<&RpcValue> {
        self.ival(Key::Params as i32)
    }
    pub fn set_param(&mut self, rv: impl Into<RpcValue>) -> &mut Self {
        self.set_param_opt(Some(rv))
    }
    pub fn with_param(mut self, param: impl Into<RpcValue>) -> Self {
        self.set_param_opt(Some(param));
        self
    }
    pub fn set_param_opt(&mut self, rv: Option<impl Into<RpcValue>>) -> &mut Self {
        self.set_ival(Key::Params, rv)
    }
    pub fn abort(&self) -> Option<AbortParam> {
        self.ival(Key::Abort as i32)
            .and_then(|v| v.try_into().ok())
    }
    pub fn set_abort(&mut self, param: AbortParam) -> &mut Self {
        self.set_ival(Key::Abort, Some(param))
    }

    pub fn response(&self) -> Result<Response<'_>, RpcError> {
        if let Some(err) = self.ival(Key::Error as i32) {
            return Err(RpcError::from_rpcvalue(err)
                .unwrap_or(RpcError {
                    code: USER_ERROR_CODE_DEFAULT.into(),
                    message: "Cannot parse 'error' key found in RPC response.".to_string(),
                }));
        }
        if let Some(progress) = self.ival(Key::Delay as i32) {
            return Ok(Response::Delay(progress.try_into().unwrap_or_default()))
        }
        Ok(Response::Success(self
            .ival(Key::Result as i32)
            .unwrap_or_else(|| STATIC_NULL_RPCVALUE.get_or_init(RpcValue::null))
        ))
    }
    pub fn set_result(&mut self, rv: impl Into<RpcValue>) -> &mut Self { self.set_ival(Key::Result, Some(rv.into())); self }
    pub fn set_result_or_error(&mut self, result: Result<RpcValue, RpcError>) -> &mut Self {
        match result {
            Ok(val) => self.set_result(val),
            Err(err) => self.set_error(err),
        }
    }
    pub fn error(&self) -> Option<RpcError> {
        self.response().err()
    }
    pub fn set_error(&mut self, err: RpcError) -> &mut Self {
        self.set_ival(Key::Error, Some(err.to_rpcvalue()))
    }
    pub fn is_success(&self) -> bool {
        matches!(self.response(), Ok(Response::Success(_)))
    }
    pub fn is_error(&self) -> bool {
        self.error().is_some()
    }
    pub fn delay(&self) -> Option<f64> {
        match self.response() {
            Ok(Response::Delay(progress)) => Some(progress),
            _ => None,
        }
    }
    pub fn set_delay(&mut self, progress: f64) -> &mut Self {
        self.set_ival(Key::Delay, Some(progress))
    }
    pub fn is_delay(&self) -> bool {
        self.delay().is_some()
    }

    pub fn meta(&self) -> &MetaMap {
        self.0.meta.as_ref().map_or_else(
            || STATIC_EMPTY_METAMAP.get_or_init(MetaMap::new),
            <Box<MetaMap>>::as_ref
        )
    }
    fn meta_mut(&mut self) -> Option<&mut MetaMap> {
        self.0.meta.as_mut().map(<Box<MetaMap>>::as_mut)
    }
    fn tag<Idx>(&self, key: Idx) -> Option<&RpcValue>
        where Idx: GetIndex
    {
        self.meta().get(key)
    }
    fn set_tag<Idx>(&mut self, key: Idx, rv: Option<impl Into<RpcValue>>) -> &mut Self
        where Idx: GetIndex
    {
        let mm = self.meta_mut().expect("Not an RpcMessage");
        match rv {
            Some(rv) => { mm.insert(key, rv.into()); }
            None => { mm.remove(key); }
        };
        self
    }
    fn ival(&self, key: i32) -> Option<&RpcValue> {
        if let Value::IMap(m) = &self.0.value {
            return m.get(&key);
        }
        None
    }
    fn set_ival(&mut self, key: Key, rv: Option<impl Into<RpcValue>>) -> &mut Self {
        if let Value::IMap(m) = &mut self.0.value {
            match rv {
                Some(rv) => m.insert(key as i32, rv.into()),
                None => m.remove(&(key as i32)),
            };
            self
        } else {
            panic!("Not an RpcMessage")
        }
    }
    pub fn new_request(shvpath: impl AsRef<str>, method: impl AsRef<str>) -> Self {
        Self::create_request_with_id(Self::next_request_id(), shvpath, method)
    }
    pub fn new_signal(shvpath: impl AsRef<str>, method: impl AsRef<str>) -> Self {
        let mut msg = Self::default();
        msg.set_shvpath(shvpath)
            .set_method(method);
        msg
    }
    pub fn new_signal_with_source(shvpath: impl AsRef<str>, method: impl AsRef<str>, source: impl AsRef<str>) -> Self {
        let mut msg = Self::default();
        msg.set_shvpath(shvpath)
            .set_method(method)
            .set_source(source);
        msg
    }
    pub fn create_request_with_id(rq_id: RqId, shvpath: impl AsRef<str>, method: impl AsRef<str>) -> Self {
        let mut msg = Self::default();
        msg.set_request_id(rq_id);
        if !shvpath.as_ref().is_empty() {
            msg.set_shvpath(shvpath);
        }
        msg.set_method(method);
        msg
    }
    pub fn prepare_response(&self) -> Result<Self, &'static str> {
        Self::prepare_response_from_meta(self.meta())
    }
    pub fn prepare_response_from_meta(meta: &MetaMap) -> Result<Self, &'static str> {
        RpcFrame::prepare_response_meta(meta).map(Self::from_meta)
    }
}
impl Default for RpcMessage {
    fn default() -> Self {
        let mut mm = MetaMap::new();
        mm.insert(rpctype::Tag::MetaTypeId as i32, RpcValue::from(rpctype::global_ns::MetaTypeID::ChainPackRpcMessage as i32));
        //mm.insert(Tag::Method as i32, RpcValue::from(method));
        RpcMessage(RpcValue::new(IMap::new().into(),Some(mm)))
    }
}

pub trait RpcMessageMetaTags {
    type Target;

    fn tag(&self, id: impl Into<i32>) -> Option<&RpcValue>;
    fn set_tag(&mut self, id: impl Into<i32>, val: Option<impl Into<RpcValue>>) -> &mut Self::Target;

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
        self.tag(Tag::RequestId as i32).map(RpcValue::as_i64)
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
    fn set_shvpath(&mut self, shv_path: impl AsRef<str>) -> &mut Self::Target {
        self.set_tag(Tag::ShvPath as i32, Some(shv_path.as_ref()))
    }
    fn method(&self) -> Option<&str> {
        self.tag(Tag::Method as i32).map(RpcValue::as_str)
    }
    fn set_method(&mut self, method: impl AsRef<str>) -> &mut Self::Target {
        self.set_tag(Tag::Method as i32, Some(method.as_ref()))
    }
    fn source(&self) -> Option<&str> {
        self.tag(Tag::Source as i32).map(RpcValue::as_str)
    }
    fn set_source(&mut self, source: impl AsRef<str>) -> &mut Self::Target {
        self.set_tag(Tag::Source as i32, Some(source.as_ref()))
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
    fn user_id(&self) -> Option<&str> {
        self.tag(Tag::UserId as i32).map(RpcValue::as_str)
    }
    fn set_user_id(&mut self, user_id: impl AsRef<str>) -> &mut Self::Target {
        self.set_tag(Tag::UserId as i32, Some(user_id.as_ref()))
    }
    fn caller_ids(&self) -> Vec<PeerId> {
        let t = self.tag(Tag::CallerIds as i32);
        match t.map(|rv| &rv.value) {
            None => Vec::new(),
            Some(Value::Int(val)) => vec![*val as PeerId],
            Some(Value::List(val)) =>
                val.iter()
                .map(|v| v.as_int() as PeerId)
                .collect(),
            _ => vec![],
        }
    }
    fn set_caller_ids(&mut self, ids: &[PeerId]) -> &mut Self::Target {
        if ids.is_empty() {
            return self.set_tag(Tag::CallerIds as i32, None::<()>);
        }
        if let &[single_id] = ids {
            return self.set_tag(Tag::CallerIds as i32, Some(RpcValue::from(single_id as PeerId)));
        }
        let lst: List = ids.iter().map(|v| RpcValue::from(*v)).collect();
        self.set_tag(Tag::CallerIds as i32, Some(RpcValue::from(lst)))
    }
    fn push_caller_id(&mut self, id: PeerId) -> &mut Self::Target {
        let mut ids = self.caller_ids();
        ids.push(id as PeerId);
        self.set_caller_ids(&ids)
    }
    fn pop_caller_id(&mut self) -> Option<PeerId> {
        let mut ids = self.caller_ids();
        let caller_id = ids.pop();
        if caller_id.is_some() {
            self.set_caller_ids(&ids);
        }
        caller_id
    }
    fn seqno(&self) -> Option<SeqNo> {
        self.tag(Tag::SeqNo as i32).map(RpcValue::as_u64)
    }
    fn set_seqno(&mut self, n: SeqNo) -> &mut Self::Target {
        self.set_tag(Tag::SeqNo as i32, Some(RpcValue::from(n)))
    }
}
impl RpcMessageMetaTags for RpcMessage {
    type Target = Self;

    fn tag(&self, id: impl Into<i32>) -> Option<&RpcValue> {
        self.tag(id.into())
    }
    fn set_tag(&mut self, id: impl Into<i32>, val: Option<impl Into<RpcValue>>) -> &mut Self::Target {
        self.set_tag(id.into(), val)
    }
}

impl RpcMessageMetaTags for MetaMap {
    type Target = MetaMap;

    fn tag(&self, id: impl Into<i32>) -> Option<&RpcValue> {
        self.get(id.into())
    }

    fn set_tag(&mut self, id: impl Into<i32>, val: Option<impl Into<RpcValue>>) -> &mut Self::Target {
        match val {
            Some(rv) => { self.insert(id.into(), rv.into()); }
            None => { self.remove(id.into()); }
        }
        self
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
    LoginRequired,
    UserIDRequired,
    NotImplemented,
    TryAgainLater,
    AbortRequestInvalid,
}
impl Display for RpcErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let s = match self {
            RpcErrorCode::NoError => "NoError",
            RpcErrorCode::InvalidRequest => "InvalidRequest",
            RpcErrorCode::MethodNotFound => "MethodNotFound",
            RpcErrorCode::InvalidParam => "InvalidParam",
            RpcErrorCode::InternalError => "InternalError",
            RpcErrorCode::ParseError => "ParseError",
            RpcErrorCode::MethodCallTimeout => "MethodCallTimeout",
            RpcErrorCode::MethodCallCancelled => "MethodCallCancelled",
            RpcErrorCode::MethodCallException => "MethodCallException",
            RpcErrorCode::PermissionDenied => "PermissionDenied",
            RpcErrorCode::LoginRequired => "LoginRequired",
            RpcErrorCode::UserIDRequired => "UserIDRequired",
            RpcErrorCode::NotImplemented => "NotImplemented",
            RpcErrorCode::TryAgainLater => "TryAgainLater",
            RpcErrorCode::AbortRequestInvalid => "AbortRequestInvalid",
        };
        write!(f, "{s}")
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

            x if x == RpcErrorCode::LoginRequired as i32 => Ok(RpcErrorCode::LoginRequired),
            x if x == RpcErrorCode::UserIDRequired as i32 => Ok(RpcErrorCode::UserIDRequired),
            x if x == RpcErrorCode::NotImplemented as i32 => Ok(RpcErrorCode::NotImplemented),
            x if x == RpcErrorCode::TryAgainLater as i32 => Ok(RpcErrorCode::TryAgainLater),
            x if x == RpcErrorCode::AbortRequestInvalid as i32 => Ok(RpcErrorCode::AbortRequestInvalid),
            _ => Err(()),
        }
    }
}

impl From<RpcErrorCode> for RpcErrorCodeKind {
    fn from(value: RpcErrorCode) -> Self {
        Self::RpcError(value)
    }
}

impl From<RpcErrorCodeKind> for u32 {
    fn from(value: RpcErrorCodeKind) -> Self {
        match value {
            RpcErrorCodeKind::RpcError(rpc_error_code) => rpc_error_code as u32,
            RpcErrorCodeKind::UserError(user_error_code) => user_error_code,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcErrorCodeKind {
    RpcError(RpcErrorCode),
    UserError(u32),
}

pub const USER_ERROR_CODE_DEFAULT: u32 = 32;

impl TryFrom<i32> for RpcErrorCodeKind {
    type Error = <i32 as TryFrom<u32>>::Error;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        RpcErrorCode::try_from(value)
            .map(Self::RpcError)
            .or_else(|()| u32::try_from(value).map(Self::UserError))
    }
}

impl From<u32> for RpcErrorCodeKind {
    fn from(value: u32) -> Self {
        RpcErrorCode::try_from(value.cast_signed()).map_or_else(|()| Self::UserError(value), RpcErrorCodeKind::from)
    }
}

impl Display for RpcErrorCodeKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RpcErrorCodeKind::RpcError(rpc_error_code) => write!(f, "{rpc_error_code}"),
            RpcErrorCodeKind::UserError(user_error_code) => write!(f, "UserError({user_error_code})"),
        }
    }
}

#[derive(Clone)]
pub struct RpcError {
    pub code: RpcErrorCodeKind,
    pub message: String,
}

enum RpcErrorKey { Code = 1, Message }

impl RpcError {
    pub fn new(code: impl Into<RpcErrorCodeKind>, msg: impl Into<String>) -> Self {
        RpcError {
            code: code.into(),
            message: msg.into(),
        }
    }
    pub fn from_rpcvalue(rv: &RpcValue) -> Option<Self> {
        if rv.is_imap() {
            let m = rv.as_imap();
            let code = m.get(&(RpcErrorKey::Code as i32)).map(RpcValue::as_u32).unwrap_or(USER_ERROR_CODE_DEFAULT);
            let msg = if let Some(msg) = m.get(&(RpcErrorKey::Message as i32)) {
                msg.as_str().to_string()
            } else {
                "".to_string()
            };
            Some(RpcError {
                code: code.into(),
                message: msg,
            })
        } else {
            None
        }
    }
    pub fn to_rpcvalue(&self) -> RpcValue {
        let mut m = IMap::new();
        m.insert(RpcErrorKey::Code as i32, RpcValue::from(u32::from(self.code).cast_signed()));
        m.insert(RpcErrorKey::Message as i32, RpcValue::from(&self.message));
        RpcValue::from(m)
    }
}
impl Default for RpcError {
    fn default() -> Self {
        RpcError {
            code: USER_ERROR_CODE_DEFAULT.into(),
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

impl Visitor<'_> for RpcMessageVisitor {
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
                Err(err) => Err(E::custom(format!("RpcMessage create error: {err}"))),
            },
            Err(err) => Err(E::custom(format!("RpcValue parse error: {err}"))),
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
        let mut rq = RpcMessage::create_request_with_id(id, "foo/bar", "baz");
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
    fn rpc_msg_delay_abort() {
        let mut rq = RpcMessage::new_request("x/y", "foo");
        rq.set_abort(super::AbortParam::Query);
        assert!(rq.abort().is_some());
        let mut resp = rq.prepare_response().unwrap();
        resp.set_delay(0.2);
        assert!(resp.delay().is_some_and(|d| d == 0.2));
        let mut resp2 = resp.clone();
        resp2.set_delay(0.3);
        assert!(resp2.delay().is_some_and(|d| d == 0.3));

    }

    #[test]
    fn rpc_msg_access_level_none() {
        let rq = RpcMessage::new_request("foo/bar", "baz");
        assert_eq!(rq.access_level(), None);
    }

    #[test]
    fn rpc_msg_access_level_some() {
        let mut rq = RpcMessage::new_request("foo/bar", "baz");
        rq.set_access_level(AccessLevel::Read);
        assert_eq!(rq.access_level(), Some(AccessLevel::Read as i32));
    }

    #[test]
    fn rpc_msg_access_level_compat() {
        let mut rq = RpcMessage::new_request("foo/bar", "baz");
        rq.set_tag(Tag::Access as i32, Some(RpcValue::from("srv")));
        assert_eq!(rq.access_level(), Some(AccessLevel::Service as i32));
    }
}
