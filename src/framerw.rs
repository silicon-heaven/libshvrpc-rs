use async_trait::async_trait;
use crate::rpcframe::{Protocol, RpcFrame};
use shvproto::{ChainPackWriter, MetaMap, RpcValue, Writer};
use crate::{RpcMessage, RpcMessageMetaTags};
use crate::rpcmessage::{RpcError, RpcErrorCode, RqId};

pub enum RpcFrameReception {
    Meta(MetaMap),
    Frame(RpcFrame),
}

#[async_trait]
pub trait FrameReader {
    async fn receive_frame_meta_data(&mut self) -> crate::Result<RpcFrameReception>;
    async fn receive_frame_meta(&mut self) -> crate::Result<MetaMap>;
    async fn receive_frame_data(&mut self) -> crate::Result<RpcFrame>;
    async fn receive_frame(&mut self) -> crate::Result<RpcFrame>;

    async fn receive_message(&mut self) -> crate::Result<RpcMessage> {
        let frame = self.receive_frame().await?;
        let msg = frame.to_rpcmesage()?;
        Ok(msg)
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

