use std::borrow::Cow;
use std::collections::BTreeMap;

use bitflags::bitflags;
use shvproto::RpcValue;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags(u32);

bitflags! {
    impl Flags: u32 {
        const None = 0;
        const IsSignal = 1 << 0;
        const IsGetter = 1 << 1;
        const IsSetter = 1 << 2;
        const LargeResultHint = 1 << 3;
        const UserIDRequired = 1 << 5;

        // The source may set any bits
        const _ = !0;
    }
}

impl Default for Flags {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<&Flags> for RpcValue {
    fn from(value: &Flags) -> Self {
        RpcValue::from(value.bits())
    }
}

impl From<Flags> for RpcValue {
    fn from(value: Flags) -> Self {
        RpcValue::from(value.bits())
    }
}

impl TryFrom<&RpcValue> for Flags {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        use shvproto::rpcvalue::Value;
        match &value.value {
            Value::Int(val) => Ok(Flags::from_bits_retain(u32::try_from(*val).map_err(|err| format!("Flags too long: {err}"))?)),
            Value::UInt(val) => Ok(Flags::from_bits_retain(u32::try_from(*val).map_err(|err| format!("Flags too long: {err}"))?)),
            _ => Err(format!("Wrong RpcValue type for Flags: {}", value.type_name())),
        }
    }
}

impl TryFrom<RpcValue> for Flags {
    type Error = String;

    fn try_from(value: RpcValue) -> Result<Self, Self::Error> {
        Flags::try_from(&value)
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq, PartialOrd)]
pub enum AccessLevel {
    #[default]
    Browse = 1,
    Read = 8,
    Write = 16,
    Command = 24,
    Config = 32,
    Service = 40,
    SuperService = 48,
    Developer = 56,
    Superuser = 63
}

impl AccessLevel {
    // It makes sense to return Option rather than Result as the `FromStr` trait does.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "bws" => Some(AccessLevel::Browse),
            "rd" => Some(AccessLevel::Read),
            "wr" => Some(AccessLevel::Write),
            "cmd" => Some(AccessLevel::Command),
            "cfg" => Some(AccessLevel::Config),
            "srv" => Some(AccessLevel::Service),
            "ssrv" => Some(AccessLevel::SuperService),
            "dev" => Some(AccessLevel::Developer),
            "su" => Some(AccessLevel::Superuser),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            AccessLevel::Browse => "bws",
            AccessLevel::Read => "rd",
            AccessLevel::Write => "wr",
            AccessLevel::Command => "cmd",
            AccessLevel::Config => "cfg",
            AccessLevel::Service => "srv",
            AccessLevel::SuperService => "ssrv",
            AccessLevel::Developer => "dev",
            AccessLevel::Superuser => "su",
        }
    }
}
impl From<&str> for AccessLevel {
    fn from(value: &str) -> Self {
        Self::from_str(value).unwrap_or(Self::Browse)
    }
}

impl TryFrom<i32> for AccessLevel {
    type Error = String;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            value if value == AccessLevel::Browse as i32 => Ok(AccessLevel::Browse),
            value if value == AccessLevel::Read as i32 => Ok(AccessLevel::Read),
            value if value == AccessLevel::Write as i32 => Ok(AccessLevel::Write),
            value if value == AccessLevel::Command as i32 => Ok(AccessLevel::Command),
            value if value == AccessLevel::Config as i32 => Ok(AccessLevel::Config),
            value if value == AccessLevel::Service as i32 => Ok(AccessLevel::Service),
            value if value == AccessLevel::SuperService as i32 => Ok(AccessLevel::SuperService),
            value if value == AccessLevel::Developer as i32 => Ok(AccessLevel::Developer),
            value if value == AccessLevel::Superuser as i32 => Ok(AccessLevel::Superuser),
            _ => Err(format!("Invalid access level: {value}")),
        }
    }
}

impl TryFrom<&RpcValue> for AccessLevel {
    type Error = String;
    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        use shvproto::rpcvalue::Value;
        match &value.value {
            Value::Int(val) => i32::try_from(*val).map_err(|err| format!("Integer value is too high for AccessLevel: {err}"))?.try_into(),
            Value::String(val) =>
                AccessLevel::from_str(val.as_str())
                .ok_or_else(|| format!("Wrong value of AccessLevel: {val}")),
            _ => Err(format!("Wrong RpcValue type for AccessLevel: {}", value.type_name())),
        }
    }
}

#[derive(Debug,Clone)]
pub enum SignalsDefinition {
    Static(&'static [(&'static str, Option<&'static str>)]),
    Dynamic(BTreeMap<String, Option<String>>),
}

impl Default for SignalsDefinition {
    fn default() -> Self {
        Self::Static(&[])
    }
}

impl From<&SignalsDefinition> for BTreeMap<String, Option<String>> {
    fn from(value: &SignalsDefinition) -> Self {
        match value {
            SignalsDefinition::Static(items) =>
                items
                .iter()
                .map(|(k, v)| (k.to_string(), v.map(String::from)))
                .collect(),
            SignalsDefinition::Dynamic(map) => map.clone(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct MetaMethod {
    pub name: Cow<'static, str>,
    pub flags: Flags,
    pub access: AccessLevel,
    pub param: Cow<'static, str>,
    pub result: Cow<'static, str>,
    pub signals: SignalsDefinition,
    pub description: Cow<'static, str>,
}

#[derive(Debug, Copy, Clone)]
pub enum DirFormat {
    IMap,
    Map,
}

impl MetaMethod {
    pub const fn new_static(
        name: &'static str,
        flags: Flags,
        access: AccessLevel,
        param: &'static str,
        result: &'static str,
        signals: &'static [(&'static str, Option<&'static str>)],
        description: &'static str
    ) -> Self {
        Self {
            name: Cow::Borrowed(name),
            flags,
            access,
            param: Cow::Borrowed(param),
            result: Cow::Borrowed(result),
            signals: SignalsDefinition::Static(signals),
            description: Cow::Borrowed(description),
        }
    }

    pub fn new(
        name: impl Into<Cow<'static, str>>,
        flags: Flags,
        access: AccessLevel,
        param: impl Into<Cow<'static, str>>,
        result: impl Into<Cow<'static, str>>,
        signals: SignalsDefinition,
        description: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            flags,
            access,
            param: param.into(),
            result: result.into(),
            signals,
            description: description.into(),
        }
    }

    pub fn to_rpcvalue(&self, fmt: DirFormat) -> RpcValue {
        fn serialize<K>(mm: &MetaMethod) -> BTreeMap<K, RpcValue>
            where
                K: From<DirAttribute>,
                K: std::cmp::Ord,
        {
                let mut m = BTreeMap::<K, RpcValue>::new();
                m.insert(DirAttribute::Name.into(), mm.name.as_ref().into());
                m.insert(DirAttribute::Flags.into(), mm.flags.bits().into());
                m.insert(DirAttribute::Param.into(), mm.param.as_ref().into());
                m.insert(DirAttribute::Result.into(), mm.result.as_ref().into());
                m.insert(DirAttribute::AccessLevel.into(), (mm.access as i32).into());
                m.insert(DirAttribute::Signals.into(), BTreeMap::from(&mm.signals).into());
                m
        }
        match fmt {
            DirFormat::IMap => {
                serialize::<i32>(self).into()
            }
            DirFormat::Map => {
                let mut m = serialize::<String>(self);
                m.insert("description".into(), self.description.as_ref().into());
                m.into()
            }
        }
    }
}

// attributes for 'dir' command
#[derive(Debug, Copy, Clone)]
pub enum DirAttribute {
    Name = 1,
    Flags,
    Param,
    Result,
    AccessLevel,
    Signals,
}
impl From<DirAttribute> for i32 {
    fn from(val: DirAttribute) -> Self {
        val as i32
    }
}
impl From<DirAttribute> for &str {
    fn from(val: DirAttribute) -> Self {
        match val {
            DirAttribute::Name => "name",
            DirAttribute::Flags => "flags",
            DirAttribute::Param => "param",
            DirAttribute::Result => "result",
            // TODO: some implementations return "accessGrant" key in the result of dir
            DirAttribute::AccessLevel => "access",
            DirAttribute::Signals => "signals",
        }
    }
}
impl From<DirAttribute> for String {
    fn from(val: DirAttribute) -> Self {
        <&str>::from(val).to_string()
    }
}

