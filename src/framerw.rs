use std::borrow::Cow;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use log::{*};
use crate::rpcframe::{rpcmsg_log_length_threshold, Protocol, RpcFrame};
use shvproto::{ChainPackWriter, MetaMap, Reader, RpcValue, Writer};
use crate::{RpcMessage, RpcMessageMetaTags};
use crate::rpcmessage::{PeerId, RpcError, RpcErrorCode, RqId};
use futures_time::future::FutureExt;
use shvproto::util::hex_dump;

#[derive(Debug)]
pub enum ReceiveFrameError {
    Timeout(Option<MetaMap>),
    FrameTooLarge(String,Option<MetaMap>),
    FramingError(String),
    StreamError(String),
}

pub fn try_chainpack_buf_to_meta(buf: &[u8]) -> Option<shvproto::MetaMap> {
    if buf.first().is_none_or(|first_byte| *first_byte != crate::rpcframe::Protocol::ChainPack as u8) {
        return None
    }
    let mut buffrd = std::io::BufReader::new(&buf[1..]);
    let mut rd = shvproto::ChainPackReader::new(&mut buffrd);
    rd.try_read_meta().ok().flatten()
}

pub(crate) fn attach_meta_to_timeout_error(err: ReceiveFrameError, data: &[u8]) -> ReceiveFrameError {
    if let ReceiveFrameError::Timeout(None) = err {
        ReceiveFrameError::Timeout(try_chainpack_buf_to_meta(data))
    } else {
        err
    }
}

impl std::fmt::Display for ReceiveFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let opt_meta_to_string = |opt_meta: &Option<MetaMap>| opt_meta
            .as_ref()
            .map_or_else(|| Cow::from("<none>"), |m| m.to_string().into());
        match self {
            ReceiveFrameError::Timeout(opt_meta) => write!(f, "Read frame timeout, frame meta: {meta}", meta = opt_meta_to_string(opt_meta)),
            ReceiveFrameError::FrameTooLarge(s, opt_meta) => write!(f, "Frame too large - {s}, frame meta: {meta}", meta = opt_meta_to_string(opt_meta)),
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
fn format_peer_id(peer_id: PeerId) -> String {
    if peer_id > 0 {
        format!("peer:{peer_id}")
    } else {
        "".to_string()
    }
}

pub(crate) async fn try_receive_frame_base(reader: &mut (impl FrameReader + ?Sized)) -> Result<RpcFrame, ReceiveFrameError> {
    let raw_data = reader.get_frame_bytes().await?;
    RpcFrame::from_raw_data(raw_data).map_err(|err| ReceiveFrameError::FramingError(err.to_string()))
}

#[async_trait]
pub trait FrameReader {
    fn peer_id(&self) -> PeerId;
    fn frame_size_limit(&self) -> usize;
    /// Read all the frame raw data
    async fn get_frame_bytes(&mut self) -> Result<Vec<u8>, ReceiveFrameError>;
    async fn try_receive_frame(&mut self) -> Result<RpcFrame, ReceiveFrameError> {
        try_receive_frame_base(self).await
    }
    async fn receive_frame(&mut self) -> Result<RpcFrame, ReceiveFrameError> {
        match self.try_receive_frame().await {
            Ok(frame) => {
               log!(target: "RpcMsg", Level::Debug, "R==> {} {}", format_peer_id(self.peer_id()), &frame);
               Ok(frame)
            }
            Err(err) => Err(err),
        }
    }
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
                return Err(ReceiveFrameError::Timeout(None));
            }
        }
    } else {
        reader.read(&mut data.data).await
    }.map_err(|e| ReceiveFrameError::StreamError(format!("Read stream error: {e}")))?;

    if n == 0 {
        Err(ReceiveFrameError::StreamError("End of stream".into()))
    } else {
        if log_enabled!(target: "RpcData", Level::Debug) {
            log_data_received(&data.data);
        }
        data.consumed = 0;
        data.length = n;
        Ok(())
    }
}

#[async_trait]
pub trait FrameWriter {
    fn peer_id(&self) -> PeerId;
    async fn send_reset_session(&mut self) -> crate::Result<()> {
        self.send_frame(RpcFrame::new_reset_session()).await
    }
    async fn send_frame_impl(&mut self, frame: RpcFrame) -> crate::Result<()>;
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()> {
        log!(target: "RpcMsg", Level::Debug, "S<== {} {}", format_peer_id(self.peer_id()), &frame.to_rpcmesage().map_or_else(|_| frame.to_string(), |rpc_msg| rpc_msg.to_string()));
        self.send_frame_impl(frame).await
    }
    async fn send_message(&mut self, msg: RpcMessage) -> crate::Result<()> {
        self.send_frame(msg.to_frame()?).await?;
        Ok(())
    }
    async fn send_error(&mut self, meta: MetaMap, errmsg: &str) -> crate::Result<()> {
        let mut msg = RpcMessage::from_meta(meta);
        msg.set_error(RpcError{ code: RpcErrorCode::MethodCallException.into(), message: errmsg.into()});
        self.send_message(msg).await
    }
    async fn send_result(&mut self, meta: MetaMap, result: RpcValue) -> crate::Result<()> {
        let mut msg = RpcMessage::from_meta(meta);
        msg.set_result(result);
        self.send_message(msg).await
    }
    async fn send_request(&mut self, shv_path: &str, method: &str, param: Option<RpcValue>) -> crate::Result<RqId> {
        self.send_request_user_id(shv_path, method, param, None).await
    }
    async fn send_request_user_id(&mut self, shv_path: &str, method: &str, param: Option<RpcValue>, user_id: Option<&str>) -> crate::Result<RqId> {
        let mut rpcmsg = RpcMessage::new_request(shv_path, method, param);
        if let Some(user_id) = user_id {
            rpcmsg.set_user_id(user_id);
        }
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

pub(crate) fn log_data_received(data: &[u8]) {
    log_data(data, "data received");
}

pub(crate) fn log_data_send(data: &[u8]) {
    log_data(data, "data send");
}

fn log_data(data: &[u8], prompt: &str) {
    let log_length_threshold = rpcmsg_log_length_threshold();
    let trimmed_at = if data.len() > log_length_threshold {
        format!(" (trimmed at {log_length_threshold})")
    } else {
        "".into()
    };
    let log_length = log_length_threshold.min(data.len());
    log!(target: "RpcData", Level::Debug, "{prompt}{trimmed_at} -------------------------\n{}", hex_dump(&data[0 .. log_length]));
}

#[cfg(test)]
pub(crate) mod test {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::task::Poll::Ready;
    use super::*;

    pub(crate) struct Chunks {
        pub(crate) chunks: Vec<Vec<u8>>,
    }
    impl AsyncRead for Chunks {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<smol::io::Result<usize>> {
            if self.chunks.is_empty() {
                return Ready(Ok(0));
            }
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
