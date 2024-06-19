use std::fs;
use std::path::Path;
use std::time::Duration;
use duration_str::parse;
use log::{info};
use serde::{Deserialize, Serialize};
use shvproto::RpcValue;
use crate::{RpcMessage};
use crate::framerw::{FrameReader, FrameWriter};
use crate::util::sha1_password_hash;

#[derive(Copy, Clone, Debug)]
pub enum LoginType {
    PLAIN,
    SHA1,
}
impl LoginType {
    pub fn to_str(&self) -> &str {
        match self {
            LoginType::PLAIN => "PLAIN",
            LoginType::SHA1 => "SHA1",
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
    pub reset_session: bool,
}

impl Default for LoginParams {
    fn default() -> Self {
        LoginParams {
            user: "".to_string(),
            password: "".to_string(),
            login_type: LoginType::SHA1,
            device_id: "".to_string(),
            mount_point: "".to_string(),
            heartbeat_interval: Duration::from_secs(60),
            reset_session: false,
        }
    }
}

impl LoginParams {
    pub fn to_rpcvalue(&self) -> RpcValue {
        let mut map = shvproto::Map::new();
        let mut login = shvproto::Map::new();
        login.insert("user".into(), RpcValue::from(&self.user));
        login.insert("password".into(), RpcValue::from(&self.password));
        login.insert("type".into(), RpcValue::from(self.login_type.to_str()));
        map.insert("login".into(), RpcValue::from(login));
        let mut options = shvproto::Map::new();
        options.insert("idleWatchDogTimeOut".into(), RpcValue::from(self.heartbeat_interval.as_secs() * 3));
        let mut device = shvproto::Map::new();
        if !self.device_id.is_empty() {
            device.insert("deviceId".into(), RpcValue::from(&self.device_id));
        } else if !self.mount_point.is_empty() {
            device.insert("mountPoint".into(), RpcValue::from(&self.mount_point));
        }
        if !device.is_empty() {
            options.insert("device".into(), RpcValue::from(device));
        }
        map.insert("options".into(), RpcValue::from(options));
        RpcValue::from(map)
    }
}

pub async fn login(frame_reader: &mut (dyn FrameReader + Send), frame_writer: &mut (dyn FrameWriter + Send), login_params: &LoginParams) -> crate::Result<i32>
{
    if login_params.reset_session {
        frame_writer.send_reset_session().await?;
    }
    let rq = RpcMessage::new_request("", "hello", None);
    frame_writer.send_message(rq).await?;
    let resp = frame_reader.receive_message().await?;
    if !resp.is_success() {
        return Err(resp.error().unwrap().to_rpcvalue().to_cpon().into());
    }
    let nonce = resp.result()?.as_map()
        .get("nonce").ok_or("Bad nonce")?.as_str();
    let hash = sha1_password_hash(login_params.password.as_bytes(), nonce.as_bytes());
    let mut login_params = login_params.clone();
    login_params.password = std::str::from_utf8(&hash)?.into();
    let rq = RpcMessage::new_request("", "login", Some(login_params.to_rpcvalue()));
    frame_writer.send_message(rq).await?;
    let resp = frame_reader.receive_message().await?;
    match resp.result()?.as_map().get("clientId") {
        None => { Ok(0) }
        Some(client_id) => { Ok(client_id.as_i32()) }
    }
}
fn default_heartbeat() -> String { "1m".into() }
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientConfig {
    pub url: String,
    pub device_id: Option<String>,
    pub mount: Option<String>,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval: String,
    pub reconnect_interval: Option<String>,
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
        let config = Default::default();
        if create_if_not_exist {
            if let Some(config_dir) = file_path.parent() {
                fs::create_dir_all(config_dir)?;
            }
            info!("Creating default config file: {file_name}");
            fs::write(file_path, serde_yaml::to_string(&config)?)?;
        }
        Ok(config)
    }
    pub fn heartbeat_interval_duration(&self) -> crate::Result<std::time::Duration> {
        Ok(parse(&self.heartbeat_interval)?)
    }
}
impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: "tcp://localhost:3755".to_string(),
            device_id: None,
            mount: None,
            heartbeat_interval: default_heartbeat(),
            reconnect_interval: None,
        }
    }
}
