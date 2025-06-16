use crate::framerw::{
    format_peer_id, serialize_meta, FrameReader, FrameReaderPrivate,
    FrameWriter, ReceiveFrameError,
};
use crate::rpcframe::RpcFrame;
use crate::rpcmessage::PeerId;
use async_trait::async_trait;
use std::io::BufReader;
use futures::{Sink, SinkExt, Stream, StreamExt};
use log::*;
use shvproto::ChainPackWriter;
use tungstenite::Message;

pub struct WebSocketFrameReader<R: Stream<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin + Send> {
    peer_id: PeerId,
    reader: R,
}
impl<R: Stream<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin + Send> WebSocketFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            peer_id: 0,
            reader,
        }
    }
}
#[async_trait]
impl<R: Stream<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin + Send> FrameReaderPrivate for WebSocketFrameReader<R> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    async fn get_frame_bytes(&mut self) -> Result<Vec<u8>, ReceiveFrameError> {
        loop {
            // Every read yields one WebSocket message
            let msg = self.reader
                .next()
                .await
                .ok_or_else(|| ReceiveFrameError::StreamError("End of stream".into()))?
                .map_err(|e| ReceiveFrameError::StreamError(format!("{e}")))?;

            match msg {
                tungstenite::Message::Binary(bytes) => {
                    let mut bufrd = BufReader::new(&bytes[..]);
                    let mut rd = shvproto::ChainPackReader::new(&mut bufrd);
                    let frame_size = rd
                        .read_uint_data()
                        .map_err(|e|
                            ReceiveFrameError::FramingError(format!("Cannot parse size of a frame: {e}"))
                        )?;
                    let frame_start = rd.position();
                    let frame = &bytes[frame_start..];
                    if frame_size != frame.len() as u64 {
                        return Err(
                            ReceiveFrameError::FramingError(
                                format!("Frame size mismatch: {} != {}", frame_size, frame.len())
                            )
                        );
                    }
                    return Ok(frame.to_vec());
                }
                tungstenite::Message::Text(utf8_bytes) =>
                    warn!("Received unsupported Text message on a WebSocket: {}", utf8_bytes),
                Message::Ping(_) => {}
                Message::Pong(_) => {}
                Message::Close(_) => {}
                Message::Frame(_) => {}
            };
        }
    }
}
#[async_trait]
impl<R: Stream<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin + Send> FrameReader for WebSocketFrameReader<R> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    fn set_peer_id(&mut self, peer_id: PeerId) {
        self.peer_id = peer_id
    }
    async fn receive_frame(&mut self) -> Result<RpcFrame, ReceiveFrameError> {
        self.receive_frame_private().await
    }
}

pub struct WebSocketFrameWriter<W: Sink<tungstenite::Message, Error = tungstenite::Error> + Unpin + Send> {
    peer_id: PeerId,
    writer: W,
}
impl<W: Sink<tungstenite::Message, Error = tungstenite::Error> + Unpin + Send> WebSocketFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { peer_id: 0, writer }
    }
}

#[async_trait]
impl<W: Sink<tungstenite::Message, Error = tungstenite::Error> + Unpin + Send> FrameWriter for WebSocketFrameWriter<W> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    fn set_peer_id(&mut self, peer_id: PeerId) {
        self.peer_id = peer_id
    }
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()> {
        log!(target: "RpcMsg", Level::Debug, "S<== {} {}", format_peer_id(self.peer_id), &frame.to_rpcmesage().map_or_else(|_| frame.to_string(), |rpc_msg| rpc_msg.to_string()));
        let meta_data = serialize_meta(&frame)?;
        let mut header = Vec::new();
        let mut wr = ChainPackWriter::new(&mut header);
        let msg_len = 1 + meta_data.len() + frame.data.len();
        wr.write_uint_data(msg_len as u64)?;
        header.push(frame.protocol as u8);
        let mut msg_data = Vec::with_capacity(header.len() + meta_data.len() + frame.data.len());
        msg_data.extend_from_slice(&header);
        msg_data.extend_from_slice(&meta_data);
        msg_data.extend_from_slice(&frame.data);
        let msg = tungstenite::Message::binary(msg_data);
        self.writer.send(msg).await.map_err(|e| format!("Cannot send a WebSocket frame: {e}").into())
    }
}

#[cfg(test)]
mod test {
    use shvproto::util::hex_dump;
    use super::*;
    use crate::util::hex_array;
    use crate::RpcMessage;
    use std::task::Poll;

    struct MockWebSocketSink<'a> {
        buf: &'a mut Vec<u8>,
    }
    impl Sink<tungstenite::Message> for MockWebSocketSink<'_> {
        type Error = tungstenite::Error;

        fn poll_ready(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, item: tungstenite::Message) -> Result<(), Self::Error> {
            *self.get_mut().buf = item.into_data().into();
            Ok(())
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn init_log() {
        let _ = env_logger::builder()
            .filter(None, LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }

    async fn frame_to_data(frame: &RpcFrame) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];
        let sink = MockWebSocketSink{ buf: &mut buff };
        {
            let mut wr = WebSocketFrameWriter::new(sink);
            wr.send_frame(frame.clone()).await.unwrap();
        }
        buff
    }

    #[async_std::test]
    async fn test_write_frame() {
        init_log();
        for msg in [
            RpcMessage::new_request("foo/bar", "baz", Some("hello".into())),
            RpcMessage::new_request("foo/bar", "baz", Some((&[0_u8; 128][..]).into())),
            RpcMessage::new_request("foo/bar", "baz", Some((&[0_u8; 5000][..]).into())),
        ] {
            let frame = msg.to_frame().unwrap();
            let buff = frame_to_data(&frame).await;
            debug!("msg: {}", msg);
            debug!("array: {}", hex_array(&buff));
            debug!("bytes:\n{}\n-------------", hex_dump(&buff));
            {
                let stream = futures::stream::iter([Ok(tungstenite::Message::binary(buff))]);
                let mut rd = WebSocketFrameReader::new(stream);
                let rd_frame = rd.receive_frame().await.unwrap();
                assert_eq!(&rd_frame, &frame);
                assert!(rd.receive_frame().await.is_err());
            }
        }
    }

    #[async_std::test]
    async fn test_ignore_non_binary_mesage() {
        init_log();
        let msg = RpcMessage::new_request("foo/bar", "baz", Some("hello".into()));
        let frame = msg.to_frame().unwrap();
        let buff = frame_to_data(&frame).await;
        debug!("msg: {}", msg);
        debug!("array: {}", hex_array(&buff));
        debug!("bytes:\n{}\n-------------", hex_dump(&buff));
        let stream = futures::stream::iter([
            Ok(tungstenite::Message::text("abc")),
            Ok(tungstenite::Message::binary(buff.clone())),
            Ok(tungstenite::Message::text("xyz")),
            Ok(tungstenite::Message::Ping(tungstenite::Bytes::new())),
            Ok(tungstenite::Message::binary(buff)),
            Ok(tungstenite::Message::text("lol")),
        ]);
        let mut rd = WebSocketFrameReader::new(stream);
        // All messages except binary are ignored
        let rd_frame = rd.receive_frame().await.unwrap();
        assert_eq!(&rd_frame, &frame);
        let rd_frame = rd.receive_frame().await.unwrap();
        assert_eq!(&rd_frame, &frame);
        // End of stream
        assert!(rd.receive_frame().await.is_err());
    }
}
