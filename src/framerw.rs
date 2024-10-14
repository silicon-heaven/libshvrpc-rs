use std::io::BufReader;
use std::mem::take;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use log::{debug, log, log_enabled, Level};
use crate::rpcframe::{Protocol, RpcFrame};
use shvproto::{ChainPackReader, ChainPackWriter, MetaMap, Reader, RpcValue, Writer};
use crate::{RpcMessage, RpcMessageMetaTags};
use crate::rpcmessage::{RpcError, RpcErrorCode, RqId};
use futures_time::future::FutureExt;
use crate::util::hex_dump;

#[derive(Debug)]
pub enum RpcFrameReception {
    Meta(MetaMap),
    FrameDataChunk { chunk: Vec<u8>, last_chunk: bool }
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
const RAW_DATA_LEN: usize = 1024 * 4;
pub(crate) struct RawData {
    pub(crate) data: [u8; RAW_DATA_LEN],
    pub(crate) consumed: usize,
    pub(crate) count: usize,
}
impl RawData {
    pub(crate) fn new() -> Self {
        Self {
            data: [0; RAW_DATA_LEN],
            consumed: 0,
            count: 0,
        }
    }
    pub(crate) fn bytes_available(&self) -> usize {
        assert!(self.count >= self.consumed);
        self.count - self.consumed
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.bytes_available() == 0
    }
}
pub(crate) struct FrameData {
    pub(crate) complete: bool,
    //pub(crate) meta: Option<MetaMap>,
    pub(crate) meta_received: bool,
    pub(crate) data: Vec<u8>,
}
#[async_trait]
pub(crate) trait FrameReaderPrivate {
    async fn get_bytes(&mut self) -> Result<(), ReceiveFrameError>;
    fn frame_data_ref_mut(&mut self) -> &mut FrameData;
    fn frame_data_ref(&self) -> &FrameData;
    fn reset_frame_data(&mut self);
    async fn receive_frame_or_meta_inner(&mut self) -> Result<RpcFrameReception, ReceiveFrameError> {
        debug!("receive_frame_or_meta_inner, frame completed: {}", self.frame_data_ref().complete);
        loop {
            if !self.frame_data_ref().complete {
                if log_enabled!(Level::Debug) {
                    let n = self.frame_data_ref().data.len();
                    self.get_bytes().await?;
                    let data = &self.frame_data_ref().data[n ..];
                    debug!("new data ++++++++++++++++++++++++++\n{}", hex_dump(data));
                } else {
                    self.get_bytes().await?;
                }
            }
            if !self.frame_data_ref().meta_received {
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
                    self.frame_data_ref_mut().meta_received = true;
                    debug!("\tMETA");
                    log!(target: "RpcMsg", Level::Debug, "R==> {}", &meta);
                    return Ok(RpcFrameReception::Meta(meta));
                } else {
                    // log!(target: "FrameReader", Level::Warn, "Meta data read error");
                    //log!(target: "FrameReader", Level::Warn, "bytes:\n{}\n-------------", crate::util::hex_dump(&self.frame_data_ref_mut().data[..]));
                    continue;
                }
            }
            let chunk = take(&mut self.frame_data_ref_mut().data);
            debug!("\tFRAME");
            log!(target: "RpcMsg", Level::Debug, "R--> chunk of {} bytes frame data", chunk.len());
            return Ok(RpcFrameReception::FrameDataChunk { chunk, last_chunk: self.frame_data_ref().complete })
        }
    }
    async fn receive_frame_or_meta_private(&mut self) -> Result<RpcFrameReception, ReceiveFrameError> {
        let ret = self.receive_frame_or_meta_inner().await;
        if ret.is_err() {
            self.reset_frame_data();
        }
        ret
    }
}
#[async_trait]
pub trait FrameReader {
    async fn receive_frame_or_meta(&mut self) -> Result<RpcFrameReception, ReceiveFrameError>;

    async fn receive_frame(&mut self) -> crate::Result<RpcFrame> {
        let mut frame = RpcFrame{
            protocol: Protocol::ChainPack,
            meta: Default::default(),
            data: vec![],
        };
        loop {
            match self.receive_frame_or_meta().await? {
                RpcFrameReception::Meta(meta) => { frame.meta = meta }
                RpcFrameReception::FrameDataChunk { mut chunk, last_chunk } => {
                    frame.data.append(&mut chunk);
                    if last_chunk {
                        return Ok(frame)
                    }
                }
            }
        }
    }

    async fn receive_message(&mut self) -> crate::Result<RpcMessage> {
        let frame = self.receive_frame().await?;
        let msg = frame.to_rpcmesage()?;
        Ok(msg)
    }
}
pub(crate) async fn read_bytes<R: AsyncRead + Unpin + Send>(reader: &mut R, data: &mut RawData, with_timeout: bool) -> Result<(), ReceiveFrameError> {
    let n = if with_timeout {
        match reader.read(&mut data.data).timeout(futures_time::time::Duration::from_secs(5)).await {
            Ok(n) => { n }
            Err(_) => {
                return Err(ReceiveFrameError::Timeout);
            }
        }
    } else {
        reader.read(&mut data.data).await
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
        if log_enabled!(target: "RpcData", Level::Debug) {
            log!(target: "RpcData", Level::Debug, "data received -------------------------\n{}", hex_dump(&data.data[0 .. n]));
        }
        data.consumed = 0;
        data.count = n;
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

#[cfg(all(test, feature = "async-std"))]
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
