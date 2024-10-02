use std::io::{BufReader};
use async_trait::async_trait;
use crate::rpcframe::{RpcFrame};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use log::*;
use shvproto::{ChainPackReader, ChainPackWriter, ReadError};
use crate::framerw::{FrameWriter, serialize_meta, ReceiveFrameError, read_bytes, RawData, FrameData, FrameReaderPrivate, FrameReader, RpcFrameReception};
use shvproto::reader::ReadErrorReason;

pub struct StreamFrameReader<R: AsyncRead + Unpin + Send> {
    reader: R,
    frame_len: usize,
    frame_data: FrameData,
    raw_data: RawData,
}
impl<R: AsyncRead + Unpin + Send> StreamFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            frame_len: 0,
            frame_data: FrameData {
                complete: false,
                meta: None,
                data: vec![],
            },
            raw_data: RawData { data: vec![], consumed: 0 },
        }
    }
    fn reset_frame(&mut self) {
        self.frame_data = FrameData {
            complete: false,
            meta: None,
            data: vec![],
        };
        self.frame_len = 0;
        self.raw_data.trim();
    }
    async fn get_raw_byte(&mut self) -> Result<u8, ReceiveFrameError> {
        if self.raw_data.raw_bytes_available() == 0 {
            read_bytes(&mut self.reader, &mut self.raw_data.data).await?;
        }
        let b = self.raw_data.data[self.raw_data.consumed];
        self.raw_data.consumed += 1;
        Ok(b)
    }
    async fn get_frame_data_byte(&mut self) -> Result<(), ReceiveFrameError> {
        if self.frame_data.data.is_empty() {
            let mut lendata: Vec<u8> = vec![];
            let frame_len = loop {
                lendata.push(self.get_raw_byte().await?);
                let mut buffrd = BufReader::new(&lendata[..]);
                let mut rd = ChainPackReader::new(&mut buffrd);
                match rd.read_uint_data() {
                    Ok(len) => { break len as usize }
                    Err(err) => {
                        let ReadError{reason, .. } = err;
                        match reason {
                            ReadErrorReason::UnexpectedEndOfStream => { continue }
                            ReadErrorReason::InvalidCharacter => { return Err(ReceiveFrameError::FrameError) }
                        }
                    }
                };
            };
            self.frame_len = frame_len;
        }
        let b = self.get_raw_byte().await?;
        self.frame_data.data.push(b);
        if self.frame_data.data.len() == self.frame_len {
            self.frame_data.complete = true;
        }
        Ok(())
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReaderPrivate for StreamFrameReader<R> {
    async fn get_byte(&mut self) -> Result<(), ReceiveFrameError> {
        self.get_frame_data_byte().await
    }

    fn can_read_meta(&self) -> bool {
        self.raw_data.raw_bytes_available() == 0 || self.frame_data.data.len() == self.frame_len
    }

    fn frame_data_ref_mut(&mut self) -> &mut FrameData {
        &mut self.frame_data
    }

    fn reset_frame_data(&mut self) {
        self.reset_frame()
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for StreamFrameReader<R> {
    async fn receive_frame_or_request_id(&mut self) -> Result<RpcFrameReception, ReceiveFrameError> {
        let ret = self.receive_frame_or_request_id_private().await;
        ret
    }
}
// fn read_frame(buff: &[u8]) -> crate::Result<RpcFrame> {
//     // log!(target: "RpcData", Level::Debug, "\n{}", hex_dump(buff));
//     let mut buffrd = BufReader::new(buff);
//     let mut rd = ChainPackReader::new(&mut buffrd);
//     let frame_len = match rd.read_uint_data() {
//         Ok(len) => { len as usize }
//         Err(err) => {
//             return Err(err.msg.into());
//         }
//     };
//     let pos = rd.position();
//     let data = &buff[pos .. pos + frame_len];
//     let protocol = if data[0] == 0 {Protocol::ResetSession} else { Protocol::ChainPack };
//     let data = &data[1 .. ];
//     let mut buffrd = BufReader::new(data);
//     let mut rd = ChainPackReader::new(&mut buffrd);
//     if let Ok(Some(meta)) = rd.try_read_meta() {
//         let pos = rd.position();
//         let frame = RpcFrame { protocol, meta, data: data[pos ..].to_vec() };
//         //log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
//         return Ok(frame);
//     }
//     Err("Meta data read error".into())
// }

pub struct StreamFrameWriter<W: AsyncWrite + Unpin + Send> {
    writer: W,
}
impl<W: AsyncWrite + Unpin + Send> StreamFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> FrameWriter for StreamFrameWriter<W> {
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()> {
        log!(target: "RpcMsg", Level::Debug, "S<== {}", &frame.to_rpcmesage().unwrap_or_default());
        let meta_data = serialize_meta(&frame)?;
        let mut header = Vec::new();
        let mut wr = ChainPackWriter::new(&mut header);
        let msg_len = 1 + meta_data.len() + frame.data.len();
        wr.write_uint_data(msg_len as u64)?;
        header.push(frame.protocol as u8);
        self.writer.write_all(&header).await?;
        self.writer.write_all(&meta_data).await?;
        self.writer.write_all(&frame.data).await?;
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.writer.flush().await?;
        Ok(())
    }
}

// fn write_frame(buff: &mut Vec<u8>, frame: RpcFrame) -> crate::Result<()> {
//     let mut meta_data = serialize_meta(&frame)?;
//     let mut header = Vec::new();
//     let mut wr = ChainPackWriter::new(&mut header);
//     let msg_len = 1 + meta_data.len() + frame.data.len();
//     wr.write_uint_data(msg_len as u64)?;
//     header.push(frame.protocol as u8);
//     let mut frame = frame;
//     buff.append(&mut header);
//     buff.append(&mut meta_data);
//     buff.append(&mut frame.data);
//     Ok(())
// }

#[cfg(all(test, feature = "async-std"))]
mod test {
    use super::*;
    use crate::util::{hex_array, hex_dump};
    use crate::{RpcMessage, RpcMessageMetaTags};
    use async_std::io::BufWriter;
    fn init_log() {
        let _ = env_logger::builder()
            .filter(None, LevelFilter::Debug)
            .is_test(true).try_init();
    }
    #[async_std::test]
    async fn test_write_frame() {
        init_log();
        let msg = RpcMessage::new_request("foo/bar", "baz", Some("hello".into()));
        let rqid = msg.request_id();

        let frame = msg.to_frame().unwrap();
        let mut buff: Vec<u8> = vec![];
        let buffwr = BufWriter::new(&mut buff);
        {
            let mut wr = StreamFrameWriter::new(buffwr);
            wr.send_frame(frame.clone()).await.unwrap();
        }
        debug!("msg: {}", msg);
        debug!("array: {}", hex_array(&buff));
        debug!("bytes:\n{}\n-------------", hex_dump(&buff));
        {
            let buffrd = async_std::io::BufReader::new(&*buff);
            let mut rd = StreamFrameReader::new(buffrd);
            let rd_frame = rd.receive_frame().await.unwrap();
            assert_eq!(&rd_frame, &frame);
        }
        {
            let buffrd = async_std::io::BufReader::new(&*buff);
            let mut rd = StreamFrameReader::new(buffrd);
            let Ok(RpcFrameReception::Meta{ request_id, .. }) = rd.receive_frame_or_request_id().await else {
                panic!("Meta should be received");
            };
            assert_eq!(request_id, rqid);
            let Ok(RpcFrameReception::Frame(rd_frame)) = rd.receive_frame_or_request_id().await else {
                panic!("Frame should be received");
            };
            assert_eq!(&rd_frame, &frame);
        }
    }
}



