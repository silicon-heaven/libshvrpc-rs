use crate::framerw::{
    format_peer_id, read_bytes, serialize_meta, FrameData, FrameReader, FrameReaderPrivate,
    FrameWriter, RawData, ReceiveFrameError,
};
use crate::rpcframe::RpcFrame;
use crate::rpcmessage::PeerId;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use log::*;
use shvproto::reader::ReadErrorReason;
use shvproto::{ChainPackReader, ChainPackWriter, ReadError};
use std::cmp::min;
use std::io::BufReader;

pub struct StreamFrameReader<R: AsyncRead + Unpin + Send> {
    peer_id: PeerId,
    reader: R,
    bytes_to_read: usize,
    frame_data: FrameData,
    raw_data: RawData,
}
impl<R: AsyncRead + Unpin + Send> StreamFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            peer_id: 0,
            reader,
            bytes_to_read: 0,
            frame_data: FrameData {
                complete: false,
                meta: None,
                data: vec![],
            },
            raw_data: RawData::new(),
        }
    }
    fn reset_frame(&mut self) {
        //debug!("RESET FRAME");
        self.frame_data = FrameData {
            complete: false,
            meta: None,
            data: vec![],
        };
        self.bytes_to_read = 0;
    }
    async fn get_raw_bytes(
        &mut self,
        count: usize,
        copy_to_frame_data: bool,
    ) -> Result<&[u8], ReceiveFrameError> {
        if self.raw_data.is_empty() {
            let with_timeout = self.bytes_to_read != 0;
            read_bytes(&mut self.reader, &mut self.raw_data, with_timeout).await?;
        }
        let n = min(count, self.raw_data.bytes_available());
        let data = &self.raw_data.data[self.raw_data.consumed..self.raw_data.consumed + n];
        self.raw_data.consumed += n;
        if copy_to_frame_data {
            self.frame_data.data.extend_from_slice(data);
            self.bytes_to_read -= data.len();
        }
        Ok(data)
    }
    async fn get_raw_byte(&mut self) -> Result<u8, ReceiveFrameError> {
        let data = self.get_raw_bytes(1, false).await?;
        Ok(data[0])
    }
    async fn get_frame_data_bytes(&mut self) -> Result<(), ReceiveFrameError> {
        if self.bytes_to_read == 0 {
            let mut lendata: Vec<u8> = vec![];
            let frame_len = loop {
                lendata.push(self.get_raw_byte().await?);
                let mut buffrd = BufReader::new(&lendata[..]);
                let mut rd = ChainPackReader::new(&mut buffrd);
                match rd.read_uint_data() {
                    Ok(len) => break len as usize,
                    Err(err) => {
                        let ReadError { reason, .. } = err;
                        match reason {
                            ReadErrorReason::UnexpectedEndOfStream => continue,
                            ReadErrorReason::InvalidCharacter => {
                                return Err(ReceiveFrameError::FrameError(
                                    "Cannot read frame length, invalid byte received".into(),
                                ))
                            }
                        }
                    }
                };
            };
            //println!("frame len: {frame_len}");
            self.reset_frame();
            self.bytes_to_read = frame_len;
            self.frame_data.data.clear();
            self.frame_data.data.reserve(frame_len);
        }
        self.get_raw_bytes(self.bytes_to_read, true).await?;
        //debug!("{}/{} byte: {:#02x}", self.frame_data.data.len(), self.frame_len, b);
        if self.bytes_to_read == 0 {
            self.frame_data.complete = true;
            //debug!("COMPLETE");
        }
        Ok(())
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReaderPrivate for StreamFrameReader<R> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    async fn get_bytes(&mut self) -> Result<(), ReceiveFrameError> {
        self.get_frame_data_bytes().await
    }

    fn frame_data_ref_mut(&mut self) -> &mut FrameData {
        &mut self.frame_data
    }
    fn frame_data_ref(&self) -> &FrameData {
        &self.frame_data
    }

    fn reset_frame_data(&mut self) {
        self.reset_frame()
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for StreamFrameReader<R> {
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
    peer_id: PeerId,
    writer: W,
}
impl<W: AsyncWrite + Unpin + Send> StreamFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { peer_id: 0, writer }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> FrameWriter for StreamFrameWriter<W> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    fn set_peer_id(&mut self, peer_id: PeerId) {
        self.peer_id = peer_id
    }
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()> {
        log!(target: "RpcMsg", Level::Debug, "S<== {} {}", format_peer_id(self.peer_id), &frame.to_rpcmesage().unwrap_or_default());
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
    use shvproto::util::hex_dump;
use super::*;
    use crate::framerw::test::from_hex;
    use crate::framerw::test::Chunks;
    use crate::util::{hex_array};
    use crate::RpcMessage;
    use async_std::io::BufWriter;
    fn init_log() {
        let _ = env_logger::builder()
            //.filter(None, LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }
    async fn frame_to_data(frame: &RpcFrame) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];
        let buffwr = BufWriter::new(&mut buff);
        {
            let mut wr = StreamFrameWriter::new(buffwr);
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
        ] {
            let frame = msg.to_frame().unwrap();

            let buff = frame_to_data(&frame).await;
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
                let rd_frame = rd.receive_frame().await.unwrap();
                assert_eq!(&rd_frame, &frame);
            }
        }
    }

    #[async_std::test]
    async fn test_read_frame_by_chunks() {
        init_log();
        for chunks in [
            // <1:1,8:5,9:"foo/bar",10:"baz">i{1:"hello"}
            vec![
                from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
            // chunk split after meta end
            vec![
                from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff"),
                from_hex("8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
            vec![
                from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61"),
                from_hex("72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c"),
                from_hex("6f ff"),
            ],
            vec![
                from_hex("21"),
                from_hex("01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61"),
                from_hex("72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c"),
                from_hex("6f ff"),
            ],
            vec![
                from_hex("21 01 8b 41 41 48 45 49"),
                from_hex("86 07 66 6f 6f 2f 62 61"),
                from_hex("72 4a 86 03 62 61 7a ff 8a"),
                from_hex("41 86 05 68 65 6c 6c"),
                from_hex("6f ff"),
            ],
        ] {
            let mut rd = StreamFrameReader::new(Chunks { chunks });
            let frame = rd.receive_frame().await;
            assert!(frame.is_ok());
        };
    }
    #[async_std::test]
    async fn test_read_two_frames_more_chunks() {
        init_log();
        //debug!("test_read_two_frames_more_chunks");
        for chunks in [
            // <1:1,8:5,9:"foo/bar",10:"baz">i{1:"hello"}
            vec![
                from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff
                          21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
            vec![
                from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff"),
                from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
            vec![
               from_hex("21 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff 21"),
               from_hex("01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff"),
               from_hex("ff 8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
        ] {
            let mut rd = StreamFrameReader::new(Chunks { chunks });
            for _ in 0 .. 2 {
                let frame = rd.receive_frame().await;
                assert!(frame.is_ok());
            }
        };
    }
    #[async_std::test]
    async fn test_read_big_frame_more_chunks() {
        init_log();
        let msg = RpcMessage::new_request("foo/bar", "baz", Some((&[0_u8; 129][..]).into()));
        // 0000 80 9e 01 8b 41 41 48 42 49 86 07 66 6f 6f 2f 62 ....AAHBI..foo/b
        // 0010 61 72 4a 86 03 62 61 7a ff 8a 41 85 80 80 00 00 arJ..baz..A.....
        // 0020 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0030 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0040 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0050 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0060 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0070 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0080 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ................
        // 0090 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 ff ................
        let data1 = frame_to_data(&msg.to_frame().unwrap()).await;
        let data1_len = data1.len();
        let meta_start = 3;
        let meta_end = 0x19;
        let mut data = data1.clone();
        data.append(&mut data1.clone());
        for chunks in [
            vec![data1.clone(), data1.clone()],
            vec![
                data[0..1].to_vec(),
                data[1..2].to_vec(),
                data[2..meta_start].to_vec(),
                data[meta_start..meta_end].to_vec(),
                data[meta_end..data1_len + 1].to_vec(),
                data[data1_len + 1..].to_vec(),
            ],
        ] {
            let mut rd = StreamFrameReader::new(Chunks { chunks });
            for _ in 0..2 {
                let frame = rd.receive_frame().await;
                assert!(frame.is_ok());
            }
        }
    }
}
