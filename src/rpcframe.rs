use std::fmt;
use std::io::BufReader;
use anyhow::anyhow;
use shvproto::{ChainPackReader, ChainPackWriter, MetaMap, RpcValue};
use shvproto::writer::Writer;
use shvproto::reader::Reader;
use crate::{RpcMessage, rpcmessage, RpcMessageMetaTags, rpctype};
use crate::util::hex_string;

#[derive(Clone, Debug)]
pub struct RpcFrame {
    pub protocol: Protocol,
    pub meta: MetaMap,
    raw_data: Vec<u8>,
    data_start: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Protocol {
    ResetSession = 0,
    ChainPack = 1,
}
impl fmt::Display for Protocol {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Protocol::ChainPack => write!(fmt, "ChainPack"),
            Protocol::ResetSession => write!(fmt, "ResetSession"),
        }
    }
}
impl PartialEq for RpcFrame {
    fn eq(&self, other: &Self) -> bool {
        self.protocol == other.protocol
            && self.meta == other.meta
            && self.data() == other.data()
    }
}

impl RpcFrame {
    pub fn new(protocol: Protocol, meta: MetaMap, data: Vec<u8>) -> Self {
        Self {
            protocol,
            meta,
            raw_data: data,
            data_start: 0
        }
    }
    pub fn new_reset_session() -> Self {
        Self {
            protocol: Protocol::ResetSession,
            meta: MetaMap::new(),
            raw_data: vec![],
            data_start: 0,
        }
    }
    pub fn data(&self) -> &[u8] {
        &self.raw_data[self.data_start..]
    }
    pub fn from_raw_data(raw_data: Vec<u8>) -> Result<Self, anyhow::Error> {
        if raw_data.is_empty() {
            return Err(anyhow!("Empty data cannot be converted to RpcFrame"));
        }
        let proto = raw_data[0];
        if proto == Protocol::ResetSession as u8 {
            return Ok(RpcFrame::new_reset_session())
        }
        if proto != Protocol::ChainPack as u8 {
            return Err(anyhow!("Invalid protocol type received {:#02x}.", proto));
        }
        let (meta, meta_len) = {
            let mut buffrd = BufReader::new(&raw_data[1..]);
            let mut rd = ChainPackReader::new(&mut buffrd);
            match rd.try_read_meta() {
                Ok(m) => {
                    if let Some(meta) = m {
                        (meta, rd.position())
                    } else {
                        return Err(anyhow!("Incomplete frame meta received."))
                    }
                }
                Err(e) => {
                    return Err(anyhow!("Frame meta parse error: {}.", e));
                }
            }
        };
        Ok(RpcFrame {
            protocol: Protocol::ChainPack,
            meta,
            raw_data,
            data_start: meta_len + 1, // meta_len + protocol_type
        })
    }
    pub fn from_rpcmessage(msg: &RpcMessage) -> crate::Result<RpcFrame> {
        let mut data = Vec::new();
        {
            let mut wr = ChainPackWriter::new(&mut data);
            wr.write_value(&msg.as_rpcvalue().value)?;
        }
        let meta = *msg.as_rpcvalue().meta.clone().unwrap_or_default();
        Ok(RpcFrame::new(Protocol::ChainPack, meta, data))
    }
    pub fn to_rpcmesage(&self) -> crate::Result<RpcMessage> {
        let mut buff = BufReader::new(self.data());
        let value = match &self.protocol {
            Protocol::ChainPack => {
                let mut rd = ChainPackReader::new(&mut buff);
                rd.read_value()?
            }
            _ => {
                return Err("Invalid protocol".into());
            }
        };
        Ok(RpcMessage::from_rpcvalue(RpcValue::new(value, Some(self.meta.clone())))?)
    }
    pub fn prepare_response_meta(src: &MetaMap) -> Result<MetaMap, &'static str> {
        if src.is_request() {
            if let Some(rqid) = src.request_id() {
                let mut dest = MetaMap::new();
                dest.insert(rpctype::Tag::MetaTypeId as i32, RpcValue::from(rpctype::GlobalNS::MetaTypeID::ChainPackRpcMessage as i32));
                dest.set_request_id(rqid);
                dest.set_caller_ids(&src.caller_ids());
                return Ok(dest)
            }
            return Err("Request ID is missing")
        }
        Err("Not RPC Request")
    }
}
impl fmt::Display for RpcFrame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if self.protocol == Protocol::ResetSession {
            write!(fmt, "RESET_SESSION")
        } else {
            write!(fmt, "{}", self.meta)?;
            if self.data().len() > 256 {
                write!(fmt, "[ ... {} bytes of data ... ]", self.data().len())
            } else {
                match RpcValue::from_chainpack(self.data()) {
                    Ok(rv) => {
                        write!(fmt, "{}", rv.to_cpon())
                    }
                    Err(e) => {
                        write!(fmt, "[{}] invalid data, unpack error: {}", hex_string(self.data(), Some(" ")), e)
                    }
                }
            }
        }
    }
}

impl rpcmessage::RpcMessageMetaTags for RpcFrame {
    type Target = RpcFrame;

    fn tag(&self, id: i32) -> Option<&RpcValue> {
        self.meta.tag(id)
    }
    fn set_tag(&mut self, id: i32, val: Option<RpcValue>) -> &mut Self::Target {
        self.meta.set_tag(id, val);
        self
    }
}
