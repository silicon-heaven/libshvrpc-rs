use std::collections::BTreeMap;

use shvproto::{RpcValue, rpcvalue};

#[derive(Debug)]
pub enum Flag {
    None = 0,
    IsSignal = 1 << 0,
    IsGetter = 1 << 1,
    IsSetter = 1 << 2,
    LargeResultHint = 1 << 3,
}
impl From<Flag> for u32 {
    fn from(val: Flag) -> Self {
        val as u32
    }
}
impl From<u8> for Flag {
    fn from(value: u8) -> Self {
        match value {
            0 => Flag::None,
            1 => Flag::IsSignal,
            2 => Flag::IsGetter,
            4 => Flag::IsSetter,
            8 => Flag::LargeResultHint,
            _ => Flag::None,
        }
    }
}
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum AccessLevel {
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
            Value::Int(val) => (*val as i32).try_into(),
            Value::String(val) =>
                AccessLevel::from_str(val.as_str())
                .ok_or_else(|| format!("Wrong value of AccessLevel: {}", val)),
            _ => Err(format!("Wrong RpcValue type for AccessLevel: {}", value.type_name())),
        }
    }
}

#[derive(Debug)]
pub struct MetaMethod {
    pub name: &'static str,
    pub flags: u32,
    pub access: AccessLevel,
    pub param: &'static str,
    pub result: &'static str,
    pub signals: &'static [(&'static str, Option<&'static str>)],
    pub description: &'static str,
}
impl Default for MetaMethod {
    fn default() -> Self {
        MetaMethod {
            name: "",
            flags: 0,
            access: AccessLevel::Browse,
            param: "",
            result: "",
            signals: &[],
            description: "",
        }
    }
}
#[derive(Debug, Copy, Clone)]
pub enum DirFormat {
    IMap,
    Map,
}
impl MetaMethod {
    pub fn to_rpcvalue(&self, fmt: DirFormat) -> RpcValue {
        match fmt {
            DirFormat::IMap => {
                let mut m = rpcvalue::IMap::new();
                m.insert(DirAttribute::Name.into(), (self.name).into());
                m.insert(DirAttribute::Flags.into(), self.flags.into());
                m.insert(DirAttribute::Param.into(), (self.param).into());
                m.insert(DirAttribute::Result.into(), (self.result).into());
                m.insert(DirAttribute::AccessLevel.into(), (self.access as i32).into());
                m.insert(DirAttribute::Signals.into(), self.signals
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.map_or_else(RpcValue::null, RpcValue::from)))
                    .collect::<BTreeMap<_,_>>()
                    .into()
                );
                m.into()
            }
            DirFormat::Map => {
                let mut m = rpcvalue::Map::new();
                m.insert(DirAttribute::Name.into(), (self.name).into());
                m.insert(DirAttribute::Flags.into(), self.flags.into());
                m.insert(DirAttribute::Param.into(), (self.param).into());
                m.insert(DirAttribute::Result.into(), (self.result).into());
                m.insert(DirAttribute::AccessLevel.into(), (self.access as i32).into());
                m.insert(DirAttribute::Signals.into(), self.signals
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.map_or_else(RpcValue::null, RpcValue::from)))
                    .collect::<BTreeMap<_,_>>()
                    .into()
                );
                m.insert("description".into(), (self.description).into());
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

