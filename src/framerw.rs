use std::io::BufReader;
use std::mem::take;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use log::{debug, log, log_enabled, Level};
use crate::rpcframe::{Protocol, RpcFrame};
use shvproto::{ChainPackReader, ChainPackWriter, MetaMap, Reader, RpcValue, Writer};
use crate::{RpcMessage, RpcMessageMetaTags};
use crate::rpcmessage::{PeerId, RpcError, RpcErrorCode, RqId};
use futures_time::future::FutureExt;
use shvproto::util::hex_dump;

#[derive(Debug)]
pub enum ReceiveFrameError {
    Timeout,
    FramingError(String),
    StreamError(String),
}

impl std::fmt::Display for ReceiveFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReceiveFrameError::Timeout => write!(f, "Read frame timeout"),
            ReceiveFrameError::FramingError(s) => write!(f, "FramingError - {s}"),
            ReceiveFrameError::StreamError(s) => write!(f, "StreamError - {s}"),
        }
    }
}

impl From<ReceiveFrameError> for crate::Error {
    fn from(value: ReceiveFrameError) -> Self {
        value.to_string().into()
    }
}

const RAW_DATA_LEN: usize = 1024 * 4;
pub(crate) struct RawData {
    pub(crate) data: [u8; RAW_DATA_LEN],
    pub(crate) consumed: usize,
    pub(crate) length: usize,
}
impl RawData {
    pub(crate) fn new() -> Self {
        Self {
            data: [0; RAW_DATA_LEN],
            consumed: 0,
            length: 0,
        }
    }
    pub(crate) fn bytes_available(&self) -> usize {
        assert!(self.length >= self.consumed);
        self.length - self.consumed
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.bytes_available() == 0
    }
}
pub(crate) struct FrameData {
    pub(crate) complete: bool,
    pub(crate) meta: Option<MetaMap>,
    pub(crate) data: Vec<u8>,
}
pub(crate) fn format_peer_id(peer_id: PeerId) -> String {
    if peer_id > 0 {
        format!("peer:{peer_id}")
    } else {
        "".to_string()
    }
}
#[async_trait]
pub(crate) trait FrameReaderPrivate {
    fn peer_id(&self) -> PeerId;
    async fn get_bytes(&mut self) -> Result<(), ReceiveFrameError>;
    fn frame_data_ref_mut(&mut self) -> &mut FrameData;
    fn frame_data_ref(&self) -> &FrameData;
    fn reset_frame_data(&mut self);
    async fn try_receive_frame(&mut self) -> Result<RpcFrame, ReceiveFrameError> {
        debug!("receive_frame_or_meta_inner, frame completed: {}", self.frame_data_ref().complete);
        loop {
            if !self.frame_data_ref().complete {
                self.get_bytes().await?;
            }
            if self.frame_data_ref().meta.is_none() {
                assert!(!self.frame_data_ref().data.is_empty());
                let proto = self.frame_data_ref().data[0];
                if proto == Protocol::ResetSession as u8 {
                    self.frame_data_ref_mut().data.drain(..1);
                    self.reset_frame_data();
                    log!(target: "RpcMsg", Level::Debug, "R==> {} RESET_SESSION", format_peer_id(self.peer_id()));
                    return Ok(RpcFrame::new_reset_session())
                }
                if proto != Protocol::ChainPack as u8 {
                    return Err(ReceiveFrameError::FramingError(format!("Invalid protocol type received {:#02x}.", proto)));
                }
                let mut buffrd = BufReader::new(&self.frame_data_ref().data[1..]);
                let mut rd = ChainPackReader::new(&mut buffrd);
                if let Ok(Some(meta)) = rd.try_read_meta() {
                    let pos = rd.position() + 1;
                    self.frame_data_ref_mut().data.drain(..pos);
                    self.frame_data_ref_mut().meta = Some(meta);
                    //debug!("\tMETA");
                } else {
                    // incomplete meta received
                }
                continue;
            }
            if self.frame_data_ref().complete {
                assert!(self.frame_data_ref().meta.is_some());
                let frame = RpcFrame {
                    protocol: Protocol::ChainPack,
                    meta: take(&mut self.frame_data_ref_mut().meta).unwrap(),
                    data: take(&mut self.frame_data_ref_mut().data),
                };
                self.reset_frame_data();
                //debug!("\tFRAME");
                log!(target: "RpcMsg", Level::Debug, "R==> {} {}", format_peer_id(self.peer_id()), &frame);
                return Ok(frame)
            }
        }
    }
    async fn receive_frame_private(&mut self) -> Result<RpcFrame, ReceiveFrameError> {
        let ret = self.try_receive_frame().await;
        if ret.is_err() {
            self.reset_frame_data();
        }
        ret
    }
}
#[async_trait]
pub trait FrameReader {
    fn peer_id(&self) -> PeerId;
    fn set_peer_id(&mut self, peer_id: PeerId);
    async fn receive_frame(&mut self) -> Result<RpcFrame, ReceiveFrameError>;
    async fn receive_message(&mut self) -> crate::Result<RpcMessage> {
        let frame = self.receive_frame().await?;
        let msg = frame.to_rpcmesage()?;
        Ok(msg)
    }
}
pub(crate) async fn read_raw_data<R: AsyncRead + Unpin + Send>(reader: &mut R, data: &mut RawData, with_timeout: bool) -> Result<(), ReceiveFrameError> {
    let n = if with_timeout {
        match reader.read(&mut data.data).timeout(futures_time::time::Duration::from_secs(5)).await {
            Ok(n) => { n }
            Err(_) => {
                return Err(ReceiveFrameError::Timeout);
            }
        }
    } else {
        reader.read(&mut data.data).await
    }.map_err(|e| ReceiveFrameError::StreamError(format!("Read stream error: {e}")))?;

    if n == 0 {
        Err(ReceiveFrameError::StreamError("End of stream".into()))
    } else {
        if log_enabled!(target: "RpcData", Level::Debug) {
            log!(target: "RpcData", Level::Debug, "data received -------------------------\n{}", hex_dump(&data.data[0 .. n]));
        }
        data.consumed = 0;
        data.length = n;
        Ok(())
    }
}
#[async_trait]
pub trait FrameWriter {
    fn peer_id(&self) -> PeerId;
    fn set_peer_id(&mut self, peer_id: PeerId);
    async fn send_reset_session(&mut self) -> crate::Result<()> {
        self.send_frame(RpcFrame::new_reset_session()).await
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

#[cfg(test)]
pub(crate) mod test {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::task::Poll::Ready;
    use async_std::io;
    use super::*;

    pub(crate) struct Chunks {
        pub(crate) chunks: Vec<Vec<u8>>,
    }
    impl AsyncRead for Chunks {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            assert!(!self.chunks.is_empty());
            let chunk = self.chunks.remove(0);
            //debug!("returning chunk: {}", hex_array(&chunk));
            assert!(buf.len() >= chunk.len());
            buf[.. chunk.len()].copy_from_slice(&chunk[..]);
            Ready(Ok(chunk.len()))
        }
    }
    pub(crate) fn from_hex(hex: &str) -> Vec<u8> {
        let mut ret = vec![];
        for s in hex.split(' ') {
            let s = s.trim();
            if s.is_empty() {
                continue;
            }
            let n = match s {
                "STX" => { 0xa2 }
                "ESTX" => { 0x02 }
                "ETX" => { 0xa3 }
                "EETX" => { 0x03 }
                "ATX" => { 0xa4 }
                "EATX" => { 0x04 }
                "ESC" => { 0xaa }
                "EESC" => { 0x0a }
                s => u8::from_str_radix(s, 16).unwrap()
            };
            ret.push(n);
        }
        ret
    }
}
