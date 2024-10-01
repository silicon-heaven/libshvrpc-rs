use std::fmt::{Debug, Display, Formatter};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use crate::rpcframe::{Protocol, RpcFrame};
use shvproto::{ChainPackWriter, MetaMap, RpcValue, Writer};
use crate::{RpcMessage, RpcMessageMetaTags};
use crate::rpcmessage::{RpcError, RpcErrorCode, RqId};
pub enum RpcFrameReception {
    ResponseId(RqId),
    Frame(RpcFrame),
}
pub enum ReceiveFrameError {
    Timeout,
    FrameError,
    StreamError,
}

impl From<ReceiveFrameError> for crate::Error {
    fn from(value: ReceiveFrameError) -> Self {
        let msg = match value {
            ReceiveFrameError::Timeout => { "Timeout" }
            ReceiveFrameError::FrameError => { "FrameError" }
            ReceiveFrameError::StreamError => { "StreamError" }
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
}
#[async_trait]
pub trait FrameReader {
    async fn try_receive_frame(&mut self) -> Result<RpcFrameReception, ReceiveFrameError>;
    async fn receive_frame(&mut self) -> crate::Result<RpcFrame> {
        loop {
            if let RpcFrameReception::Frame(frame) = self.try_receive_frame().await? {
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
pub(crate) async fn read_bytes<R: AsyncRead + Unpin + Send>(reader: &mut R, data: &mut Vec<u8>) -> Result<(), ReceiveFrameError> {
    const BUFF_LEN: usize = 1024 * 4;
    let mut buff = [0; BUFF_LEN];
    let Ok(n) = reader.read(&mut buff).await else {
        return Err(ReceiveFrameError::StreamError);
    };
    if n == 0 {
        Err(ReceiveFrameError::StreamError)
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

