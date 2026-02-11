use std::fs;
use std::path::Path;
use std::time::Duration;
use duration_str::HumanFormat;
use log::info;
use serde::{Deserialize, Serialize};
use shvproto::RpcValue;
use url::Url;
use crate::RpcMessage;
use crate::framerw::{FrameReader, FrameWriter};
use crate::util::sha1_password_hash;
use log::debug;
use crate::rpcframe::Protocol;
use crate::rpcmessage::{Response, RqId};
use crate::rpcmessage::RpcMessageMetaTags;

#[derive(Copy, Clone, Debug)]
pub enum LoginType {
    Plain,
    Sha1,
}
impl LoginType {
    pub fn to_str(&self) -> &str {
        match self {
            LoginType::Plain => "PLAIN",
            LoginType::Sha1 => "SHA1",
        }
    }
}

 pub enum Scheme {
     Tcp,
     LocalSocket,
}

#[derive(Clone, Debug)]
pub struct LoginParams {
    pub user: String,
    pub password: String,
    pub login_type: LoginType,
    pub device_id: String,
    pub mount_point: String,
    pub heartbeat_interval: Duration,
    pub user_agent: String,
}

impl Default for LoginParams {
    fn default() -> Self {
        LoginParams {
            user: "".to_string(),
            password: "".to_string(),
            login_type: LoginType::Sha1,
            device_id: "".to_string(),
            mount_point: "".to_string(),
            heartbeat_interval: Duration::from_secs(60),
            user_agent: "".to_string(),
        }
    }
}

impl From<LoginParams> for RpcValue {
    fn from(value: LoginParams) -> Self {
        let mut map = shvproto::Map::new();
        let mut login = shvproto::Map::new();
        login.insert("user".into(), RpcValue::from(value.user));
        login.insert("password".into(), RpcValue::from(value.password));
        login.insert("type".into(), RpcValue::from(value.login_type.to_str()));
        map.insert("login".into(), RpcValue::from(login));
        let mut options = shvproto::Map::new();
        options.insert("idleWatchDogTimeOut".into(), RpcValue::from((value.heartbeat_interval.as_secs() * 3).cast_signed()));
        let mut device = shvproto::Map::new();
        if !value.device_id.is_empty() {
            device.insert("deviceId".into(), RpcValue::from(value.device_id));
        } else if !value.mount_point.is_empty() {
            device.insert("mountPoint".into(), RpcValue::from(value.mount_point));
        }
        if !device.is_empty() {
            options.insert("device".into(), RpcValue::from(device));
        }
        if !value.user_agent.is_empty() {
            options.insert("userAgent".into(), RpcValue::from(value.user_agent));
        }
        map.insert("options".into(), RpcValue::from(options));
        RpcValue::from(map)
    }
}

impl From<&LoginParams> for RpcValue {
    fn from(value: &LoginParams) -> Self {
        value.clone().into()
    }
}

pub async fn login(frame_reader: &mut (dyn FrameReader + Send), frame_writer: &mut (dyn FrameWriter + Send), login_params: &LoginParams, reset_session: bool) -> crate::Result<i32> {
    async fn get_response(rqid: Option<RqId>, frame_reader: &mut (dyn FrameReader + Send)) -> crate::Result<Option<RpcMessage>> {
        let Some(rqid) = rqid else {
            return Err("BUG: request id should be set".into());
        };
        loop {
            let frame = frame_reader.receive_frame().await?;
            if frame.protocol == Protocol::ResetSession {
                return Ok(None);
            }
            let resp = frame.to_rpcmesage()?;
            if resp.request_id().unwrap_or_default() != rqid {
                continue;
            }
            if resp.is_delay() {
                continue;
            }
            return Ok(Some(resp))
        };
    }
    debug!("Login sequence started");
    if reset_session {
        debug!("\t reset session");
        frame_writer.send_reset_session().await?;
    }
    'session_loop: loop {
        debug!("\t send hello");
        let rq = RpcMessage::new_request("", "hello");
        let hello_rq_id = rq.request_id();
        frame_writer.send_message(rq).await?;
        let resp = match get_response(hello_rq_id, frame_reader).await? {
            None => continue 'session_loop,
            Some(resp) => resp,
        };
        let Ok(Response::Success(result)) = resp.response() else {
            return Err(resp.error().expect("An error message received").to_rpcvalue().to_cpon().into());
        };
        let nonce = result
            .as_map()
            .get("nonce")
            .ok_or("Bad nonce")?
            .as_str();
        debug!("\t nonce received: {nonce}");
        let mut login_params = login_params.clone();
        if matches!(login_params.login_type, LoginType::Sha1) {
            login_params.password = sha1_password_hash(login_params.password.as_bytes(), nonce.as_bytes());
        }
        let rq = RpcMessage::new_request("", "login").with_param(login_params);
        let login_rq_id = rq.request_id();
        debug!("\t send login");
        frame_writer.send_message(rq).await?;

        let resp = match get_response(login_rq_id, frame_reader).await? {
            None => continue 'session_loop,
            Some(resp) => resp,
        };

        debug!("\t login response result: {:?}", resp.response());
        let Ok(Response::Success(result)) = resp.response() else {
            return Err(format!("Login error: {}", resp.error().expect("An error message received")).into());
        };
        match result.as_map().get("clientId") {
            None => return Ok(0),
            Some(client_id) => return Ok(client_id.as_i32()),
        }
    }
}


fn default_heartbeat() -> Duration { Duration::from_secs(60) }

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ClientConfig {
    pub url: Url,
    pub device_id: Option<String>,
    pub mount: Option<String>,
    #[serde(
        default = "default_heartbeat",
        deserialize_with = "duration_str::deserialize_duration",
        serialize_with = "serialize_duration_as_string"
    )]
    pub heartbeat_interval: Duration,
    #[serde(
        default,
        deserialize_with = "duration_str::deserialize_duration",
        serialize_with = "serialize_option_duration_as_string"
    )]
    pub reconnect_interval: Option<Duration>,
}

pub fn serialize_duration_as_string<S>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    duration.human_format().serialize(serializer)
}

pub fn serialize_option_duration_as_string<S>(
    duration_opt: &Option<Duration>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    duration_opt
        .map(|duration| duration.human_format())
        .serialize(serializer)
}

impl ClientConfig {
    pub fn from_file(file_name: &str) -> crate::Result<Self> {
        let content = fs::read_to_string(file_name)?;
        Ok(serde_yaml::from_str(&content)?)
    }
    pub fn from_file_or_default(file_name: &str, create_if_not_exist: bool) -> crate::Result<Self> {
        let file_path = Path::new(file_name);
        if file_path.exists() {
            info!("Loading config file {file_name}");
            return match Self::from_file(file_name) {
                Ok(cfg) => {
                    Ok(cfg)
                }
                Err(err) => {
                    Err(format!("Cannot read config file: {file_name} - {err}").into())
                }
            }
        } else if !create_if_not_exist {
            return Err(format!("Cannot find config file: {file_name}").into())
        }
        let config = ClientConfig::default();
        if create_if_not_exist {
            if let Some(config_dir) = file_path.parent() {
                fs::create_dir_all(config_dir)?;
            }
            info!("Creating default config file: {file_name}");
            fs::write(file_path, serde_yaml::to_string(&config)?)?;
        }
        Ok(config)
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: Url::parse("tcp://localhost:3755").expect("BUG: default URL should be valid"),
            device_id: None,
            mount: None,
            heartbeat_interval: default_heartbeat(),
            reconnect_interval: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use url::Url;

    use super::ClientConfig;

    #[test]
    fn client_config_serde() {
        let input_yaml =
r#"
url: tcp:://user@localhost:3755?password=secret
heartbeat_interval: 1m
reconnect_interval: 3s
"#;

        let config = ClientConfig {
            url: Url::parse("tcp:://user@localhost:3755?password=secret").unwrap(),
            device_id: None,
            mount: None,
            heartbeat_interval: Duration::from_secs(60),
            reconnect_interval: Some(Duration::from_secs(3)),
        };

        let parsed_config: ClientConfig = serde_yaml::from_str(input_yaml).expect("Cannot deserialize");
        assert_eq!(config, parsed_config);
    }
}
