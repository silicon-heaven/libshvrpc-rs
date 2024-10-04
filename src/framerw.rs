use std::io::BufReader;
use std::mem::take;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use log::{log, Level};
use crate::rpcframe::{Protocol, RpcFrame};
use shvproto::{ChainPackReader, ChainPackWriter, MetaMap, Reader, RpcValue, Writer};
use crate::{RpcMessage, RpcMessageMetaTags};
use crate::rpcmessage::{RpcError, RpcErrorCode, RqId};
use futures_time::future::FutureExt;

#[derive(Debug)]
pub enum RpcFrameReception {
    Meta { request_id: Option<RqId>, shv_path: Option<String>, method: Option<String>, source: Option<String> },
    Frame(RpcFrame),
}
impl RpcFrameReception {
    fn from_meta(meta: &MetaMap) -> Self {
        Self::Meta {
            request_id: meta.request_id(),
            shv_path: meta.shv_path().map(|s| s.to_string()),
            method: meta.method().map(|s| s.to_string()),
            source: meta.source().map(|s| s.to_string()),
        }
    }
}

#[derive(Debug)]
pub enum ReceiveFrameError {
    Timeout,
    FrameError(String),
    StreamError(String),
}

impl From<ReceiveFrameError> for crate::Error {
    fn from(value: ReceiveFrameError) -> Self {
        let msg: String = match value {
            ReceiveFrameError::Timeout => { "Read frame timeout".into() }
            ReceiveFrameError::FrameError(s) => { format!("FrameError - {s}") }
            ReceiveFrameError::StreamError(s) => { format!("StreamError - {s}") }
        };
        msg.into()
    }
}
pub(crate) struct RawData {
    pub(crate) data: Vec<u8>,
    pub(crate) consumed: usize,
}
impl RawData {
    pub(crate) fn raw_bytes_available(&self) -> usize {
        assert!(self.data.len() >= self.consumed);
        self.data.len() - self.consumed
    }
    pub(crate) fn trim(&mut self) {
        self.data.drain( .. self.consumed);
        self.consumed = 0;
    }
}
pub(crate) struct FrameData {
    pub(crate) complete: bool,
    pub(crate) meta: Option<MetaMap>,
    pub(crate) data: Vec<u8>,
}
#[async_trait]
pub(crate) trait FrameReaderPrivate {
    async fn get_byte(&mut self) -> Result<(), ReceiveFrameError>;
    fn can_read_meta(&self) -> bool;
    fn frame_data_ref_mut(&mut self) -> &mut FrameData;
    fn frame_data_ref(&self) -> &FrameData;
    fn reset_frame_data(&mut self);
    async fn receive_frame_or_request_id_inner(&mut self) -> Result<RpcFrameReception, ReceiveFrameError> {
        loop {
            if !self.frame_data_ref().complete {
                self.get_byte().await?;
            }
            if self.frame_data_ref().meta.is_none() && self.can_read_meta() {
                let proto = self.frame_data_ref().data[0];
                if proto == Protocol::ResetSession as u8 {
                    return Err(ReceiveFrameError::StreamError("Reset session message received.".into()));
                }
                if proto != Protocol::ChainPack as u8 {
                    return Err(ReceiveFrameError::FrameError(format!("Invalid protocol type received {:#02x}.", proto)));
                }
                let mut buffrd = BufReader::new(&self.frame_data_ref().data[1..]);
                let mut rd = ChainPackReader::new(&mut buffrd);
                if let Ok(Some(meta)) = rd.try_read_meta() {
                    let pos = rd.position() + 1;
                    self.frame_data_ref_mut().data.drain(..pos);
                    let rmeta = RpcFrameReception::from_meta(&meta);
                    self.frame_data_ref_mut().meta = Some(meta);
                    return Ok(rmeta);
                } else {
                    // log!(target: "FrameReader", Level::Warn, "Meta data read error");
                    //log!(target: "FrameReader", Level::Warn, "bytes:\n{}\n-------------", crate::util::hex_dump(&self.frame_data_ref_mut().data[..]));
                    continue;
                }
            }
            if self.frame_data_ref().complete {
                assert!(self.frame_data_ref().meta.is_some());
                let frame = RpcFrame {
                    protocol: Protocol::ChainPack,
                    meta: take(&mut self.frame_data_ref_mut().meta).unwrap(),
                    data: take(&mut self.frame_data_ref_mut().data),
                };
                self.reset_frame_data();
                log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
                return Ok(RpcFrameReception::Frame(frame))
            }
        }
    }
    async fn receive_frame_or_request_id_private(&mut self) -> Result<RpcFrameReception, ReceiveFrameError> {
        let ret = self.receive_frame_or_request_id_inner().await;
        if ret.is_err() {
            self.reset_frame_data();
        }
        ret
    }
}
#[async_trait]
pub trait FrameReader {
    async fn receive_frame_or_request_id(&mut self) -> Result<RpcFrameReception, ReceiveFrameError>;

    async fn receive_frame(&mut self) -> crate::Result<RpcFrame> {
        loop {
            if let RpcFrameReception::Frame(frame) = self.receive_frame_or_request_id().await? {
                return Ok(frame);
            }
        }
    }

    async fn receive_message(&mut self) -> crate::Result<RpcMessage> {
        let frame = self.receive_frame().await?;
        let msg = frame.to_rpcmesage()?;
        Ok(msg)
    }
}
pub(crate) async fn read_bytes<R: AsyncRead + Unpin + Send>(reader: &mut R, data: &mut Vec<u8>, with_timeout: bool) -> Result<(), ReceiveFrameError> {
    const BUFF_LEN: usize = 1024 * 4;
    let mut buff = [0; BUFF_LEN];
    let n = if with_timeout {
        match reader.read(&mut buff).timeout(futures_time::time::Duration::from_secs(5)).await {
            Ok(n) => { n }
            Err(_) => {
                return Err(ReceiveFrameError::Timeout);
            }
        }
    } else {
        reader.read(&mut buff).await
    };
    let n = match n {
        Ok(n) => { n }
        Err(e) => {
            return Err(ReceiveFrameError::StreamError(format!("Read stream error: {e}")));
        }
    };
    if n == 0 {
        Err(ReceiveFrameError::StreamError("End of stream".into()))
    } else {
        data.extend_from_slice(&buff[..n]);
        Ok(())
    }
}
#[async_trait]
pub trait FrameWriter {
    async fn send_reset_session(&mut self) -> crate::Result<()> {
        self.send_frame(RpcFrame{
            protocol: Protocol::ResetSession,
            meta: MetaMap::new(),
            data: vec![],
        }).await
    }
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()>;
    async fn send_message(&mut self, msg: RpcMessage) -> crate::Result<()> {
        self.send_frame(msg.to_frame()?).await?;
        Ok(())
    }
    async fn send_error(&mut self, meta: MetaMap, errmsg: &str) -> crate::Result<()> {
        let mut msg = RpcMessage::from_meta(meta);
        msg.set_error(RpcError{ code: RpcErrorCode::MethodCallException, message: errmsg.into()});
        self.send_message(msg).await
    }
    async fn send_result(&mut self, meta: MetaMap, result: RpcValue) -> crate::Result<()> {
        let mut msg = RpcMessage::from_meta(meta);
        msg.set_result(result);
        self.send_message(msg).await
    }
    async fn send_request(&mut self, shv_path: &str, method: &str, param: Option<RpcValue>) -> crate::Result<RqId> {
        let rpcmsg = RpcMessage::new_request(shv_path, method, param);
        let rqid = rpcmsg.request_id().expect("Request ID should exist here.");
        self.send_message(rpcmsg).await?;
        Ok(rqid)
    }
}

pub fn serialize_meta(frame: &RpcFrame) -> crate::Result<Vec<u8>> {
    let data = match frame.protocol {
        Protocol::ResetSession => {
            Vec::new()
        }
        Protocol::ChainPack => {
            let mut data: Vec<u8> = Vec::new();
            let mut wr = ChainPackWriter::new(&mut data);
            wr.write_meta(&frame.meta)?;
            data
        }
    };
    Ok(data)
}

