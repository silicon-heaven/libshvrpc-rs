use shvproto::RpcValue;

use crate::metamethod::{AccessLevel, DirAttribute};

#[derive(Debug,PartialEq)]
pub enum LsParam {
    List,
    Exists(String),
}

impl From<&RpcValue> for LsParam {
    fn from(value: &RpcValue) -> Self {
        match value.value() {
            shvproto::Value::String(dirname) => LsParam::Exists(String::clone(dirname)),
            _ => LsParam::List,
        }
    }
}

impl From<Option<&RpcValue>> for LsParam {
    fn from(value: Option<&RpcValue>) -> Self {
        value.map_or(LsParam::List, |rpcval| rpcval.into())
    }
}

impl From<LsParam> for RpcValue {
    fn from(value: LsParam) -> Self {
        match value {
            LsParam::List => ().into(),
            LsParam::Exists(dirname) => dirname.into(),
        }
    }
}

#[derive(Debug)]
pub enum LsResult {
    Exists(bool),
    List(Vec<String>),
}

impl TryFrom<&RpcValue> for LsResult {
    type Error = String;
    fn try_from(rpcvalue: &RpcValue) -> Result<Self, Self::Error> {
        match rpcvalue.value() {
            shvproto::Value::Bool(false) | shvproto::Value::Null => Ok(LsResult::Exists(false)),
            shvproto::Value::Bool(true) => Ok(LsResult::Exists(true)),
            shvproto::Value::List(_) => Ok(LsResult::List(rpcvalue.try_into()?)),
            _ => Err(format!("Wrong RpcValue type for a result of `ls`: {}", rpcvalue.type_name()))
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DirParam {
    Brief,
    Full,
    BriefMethod(String),
}

impl From<&RpcValue> for DirParam {
    fn from(rpcvalue: &RpcValue) -> Self {
        match rpcvalue.value() {
            shvproto::Value::String(param) => DirParam::BriefMethod(param.to_string()),
            shvproto::Value::Bool(true) => DirParam::Full,
            _ => DirParam::Brief,
        }

    }
}

impl From<Option<&RpcValue>> for DirParam {
    fn from(value: Option<&RpcValue>) -> Self {
        value.map_or(DirParam::Brief, |rpcval| rpcval.into())
    }
}

impl From<DirParam> for RpcValue {
    fn from(value: DirParam) -> Self {
        match value {
            DirParam::Brief => false.into(),
            DirParam::Full => true.into(),
            DirParam::BriefMethod(method) => method.into(),
        }
    }
}


#[derive(Debug,PartialEq)]
pub struct MethodInfo {
    pub name: String,
    pub flags: u32,
    pub access_level: AccessLevel,
    pub param: String,
    pub result: String,
}

impl TryFrom<&RpcValue> for MethodInfo {
    type Error = String;
    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        match value.value() {
            shvproto::Value::Map(map) => {
                let get_key = |key: DirAttribute| {
                    map.get(key.into()).ok_or_else(|| format!("Missing MethodInfo key `{}` in Map", <&str>::from(key)))
                };
                let format_err = |field: DirAttribute, err: &String| {
                    let field_name: &str = field.into();
                    format!("Invalid MethodInfo field `{field_name}` in Map: {err}")
                };
                Ok(MethodInfo {
                    name: get_key(DirAttribute::Name)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Name, &e))?,
                    flags: get_key(DirAttribute::Flags)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Flags, &e))?,
                    access_level: map.get("access").or_else(|| map.get("accessGrant"))
                        .ok_or("Missing MethodInfo key `access` or `accessGrant` in Map")?
                        .try_into()
                        .map_err(|err| format!("Invalid MethodInfo field `access` or `accessGrant` in Map: {err}"))?,
                    param: get_key(DirAttribute::Param)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Param, &e))?,
                    result: get_key(DirAttribute::Result)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Result, &e))?,
                })
            }
            shvproto::Value::IMap(imap) => {
                let get_key = |key: DirAttribute| {
                    imap.get(&i32::from(key)).ok_or_else(||
                        format!("Missing MethodInfo key `{}`({}) in IMap", i32::from(key), <&str>::from(key)).to_string()
                    )
                };
                let format_err = |field: DirAttribute, err: &String| {
                    let field_name: &str = field.into();
                    let field_num: i32 = field.into();
                    format!("Invalid MethodInfo field `{field_name}`({field_num}) in IMap: {err}")
                };
                Ok(MethodInfo {
                    name: get_key(DirAttribute::Name)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Name, &e))?,
                    flags: get_key(DirAttribute::Flags)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Flags, &e))?,
                    access_level: get_key(DirAttribute::AccessLevel)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::AccessLevel, &e))?,
                    param: get_key(DirAttribute::Param)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Param, &e))?,
                    result: get_key(DirAttribute::Result)?
                        .try_into()
                        .map_err(|e| format_err(DirAttribute::Result, &e))?,
                })
            }
            _ => Err(format!("Wrong RpcValue type for MethodInfo: {}", value.type_name())),
        }
    }
}

#[derive(Debug)]
pub enum DirResult {
    Exists(bool),
    List(Vec<MethodInfo>),
}

impl TryFrom<&RpcValue> for DirResult {
    type Error = String;
    fn try_from(rpcvalue: &RpcValue) -> Result<Self, Self::Error> {
        match rpcvalue.value() {
            shvproto::Value::Bool(false) | shvproto::Value::Null => Ok(DirResult::Exists(false)),
            shvproto::Value::Bool(true) => Ok(DirResult::Exists(true)),
            shvproto::Value::Map(_) | shvproto::Value::IMap(_) =>
                MethodInfo::try_from(rpcvalue).map(|_| DirResult::Exists(true)),
            shvproto::Value::List(_) => Ok(DirResult::List(rpcvalue.try_into()?)),
            _ => Err(format!("Wrong RpcValue type: {}", rpcvalue.type_name()))
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::metamethod::Flag;

    use super::*;

    #[test]
    fn dir_param_from_rpcvalue() {
        assert_eq!(DirParam::Brief, (&RpcValue::from(())).into());
        assert_eq!(DirParam::Brief, (&RpcValue::from(false)).into());
        assert_eq!(DirParam::BriefMethod("foo".into()), (&RpcValue::from("foo")).into());
        assert_eq!(DirParam::Full, (&RpcValue::from(true)).into());
    }

    #[test]
    fn dir_param_into_rpcvalue() {
        assert_eq!(RpcValue::from(DirParam::Brief), false.into());
        assert_eq!(RpcValue::from(DirParam::BriefMethod("foo".into())), "foo".into());
        assert_eq!(RpcValue::from(DirParam::Full), true.into());
    }

    fn method_info() -> MethodInfo {
        MethodInfo {
            name: "method".to_string(),
            flags: Flag::IsGetter.into(),
            access_level: AccessLevel::Read,
            param: "param".to_string(),
            result: "result".to_string(),
        }
    }

    #[test]
    fn method_info_from_rpcvalue() {
        let rv_map: RpcValue = shvproto::make_map!(
            "name" => "method",
            "flags" => Flag::IsGetter as u32,
            "access" => "rd",
            "param" => "param",
            "result" => "result",
        ).into();
        assert_eq!(method_info(), (&rv_map).try_into().unwrap());

        let rv_map: RpcValue = shvproto::make_map!(
            "name" => "method",
            "flags" => Flag::IsGetter as u32,
            "accessGrant" => "rd",
            "param" => "param",
            "result" => "result",
        ).into();
        assert_eq!(method_info(), (&rv_map).try_into().unwrap());

        let rv_imap: RpcValue = [
            (i32::from(DirAttribute::Name), RpcValue::from("method")),
            (i32::from(DirAttribute::Flags), RpcValue::from(Flag::IsGetter as u32)),
            (i32::from(DirAttribute::AccessLevel), RpcValue::from(AccessLevel::Read as i32)),
            (i32::from(DirAttribute::Param), RpcValue::from("param")),
            (i32::from(DirAttribute::Result), RpcValue::from("result")),
        ].into_iter().collect::<BTreeMap::<_,_>>().into();
        assert_eq!(method_info(), (&rv_imap).try_into().unwrap());
    }

    #[test]
    #[should_panic]
    fn method_info_from_rpcvalue_missing_field() {
        let rv_map: RpcValue = shvproto::make_map!(
            "name" => "method",
            "flags" => Flag::IsGetter as u32,
            // "access" => AccessLevel::Read as i32,
            "param" => "param",
            "result" => "result",
        ).into();
        let _:MethodInfo = (&rv_map).try_into().unwrap();
    }

    #[test]
    #[should_panic]
    fn method_info_from_rpcvalue_wrong_field_type() {
        let rv_map: RpcValue = shvproto::make_map!(
            "name" => "method",
            "flags" => Flag::IsGetter as u32,
            "access" => AccessLevel::Read as i32,
            "param" => (),
            "result" => "result",
        ).into();
        let _:MethodInfo = (&rv_map).try_into().unwrap();
    }

    #[test]
    fn ls_param_from_rpcvalue() {
        assert_eq!(LsParam::List, (&RpcValue::from(())).into());
        assert_eq!(LsParam::List, (&RpcValue::from(false)).into());
        assert_eq!(LsParam::List, None.into());
        assert_eq!(LsParam::Exists("foo".into()), (&RpcValue::from("foo")).into());
    }

    #[test]
    fn ls_param_into_rpcvalue() {
        assert_eq!(RpcValue::from(LsParam::List), ().into());
        assert_eq!(RpcValue::from(LsParam::Exists("foo".into())), "foo".into());
    }
}
