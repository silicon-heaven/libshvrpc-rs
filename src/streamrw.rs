use crate::framerw::{
    attach_meta_to_timeout, read_raw_data, serialize_meta, try_chainpack_buf_to_meta, FrameReader, FrameWriter, RawData, ReceiveFrameError
};
use crate::rpcframe::{Protocol, RpcFrame};
use crate::rpcmessage::PeerId;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use shvproto::reader::ReadErrorReason;
use shvproto::{ChainPackReader, ChainPackWriter, ReadError};
use std::cmp::min;
use std::io::BufReader;

pub(crate) const DEFAULT_FRAME_SIZE_LIMIT: usize = 50 * 1024 * 1024;

pub struct StreamFrameReader<R: AsyncRead + Unpin + Send> {
    peer_id: PeerId,
    reader: R,
    raw_data: RawData,
    frame_size_limit: usize,
}
impl<R: AsyncRead + Unpin + Send> StreamFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            peer_id: 0,
            reader,
            raw_data: RawData::new(),
            frame_size_limit: DEFAULT_FRAME_SIZE_LIMIT,
        }
    }
    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = peer_id;
        self
    }

    pub fn with_frame_size_limit(mut self, frame_size_limit: usize) -> Self {
        self.frame_size_limit = frame_size_limit;
        self
    }

    async fn get_raw_bytes(&mut self, count: usize) -> Result<&[u8], ReceiveFrameError> {
        if self.raw_data.is_empty() {
            read_raw_data(&mut self.reader, &mut self.raw_data, false).await?;
        }
        let n = min(count, self.raw_data.bytes_available());
        let data = &self.raw_data.data[self.raw_data.consumed..self.raw_data.consumed + n];
        self.raw_data.consumed += n;
        assert!(self.raw_data.consumed <= self.raw_data.length);
        Ok(data)
    }
    async fn get_raw_byte(&mut self) -> Result<u8, ReceiveFrameError> {
        let data = self.get_raw_bytes(1).await?;
        Ok(data[0])
    }
    async fn get_frame_bytes_impl(&mut self) -> Result<Vec<u8>, ReceiveFrameError> {
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
                            return Err(ReceiveFrameError::FramingError(
                                "Cannot read frame length, invalid byte received".into()
                            ))
                        }
                    }
                }
            };
        };
        if frame_len == 0 {
            return Err(ReceiveFrameError::FramingError("Frame length cannot be 0.".into()))
        }
        let mut bytes_to_read = frame_len.min(self.frame_size_limit());
        let mut data = Vec::with_capacity(bytes_to_read);
        while bytes_to_read > 0 {
            let bytes = self.get_raw_bytes(bytes_to_read).await.map_err(|err| attach_meta_to_timeout(err, &data))?;
            assert!(!bytes.is_empty()); // get_raw_bytes() never returns 0
            assert!(bytes.len() <= bytes_to_read);
            let first_chunk = data.is_empty();
            if first_chunk {
                let protocol = bytes[0];
                if protocol > Protocol::ChainPack as u8 {
                    return Err(ReceiveFrameError::FramingError(format!("Invalid protocol type received: {protocol}")))
                }
            }
            bytes_to_read -= bytes.len();
            data.extend_from_slice(bytes);
            if data.len() >= self.frame_size_limit() {
                return Err(ReceiveFrameError::FrameTooLarge(
                        format!("Client ID: {}, Jumbo frame of {frame_len} bytes is not supported. Jumbo frame threshold is {frame_size_limit} bytes.",
                            self.peer_id,
                            frame_size_limit = self.frame_size_limit()
                        ),
                        try_chainpack_buf_to_meta(&data))
                )

            }
        }
        Ok(data)
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for StreamFrameReader<R> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn frame_size_limit(&self) -> usize {
        self.frame_size_limit
    }

    async fn get_frame_bytes(&mut self) -> Result<Vec<u8>, ReceiveFrameError> {
        self.get_frame_bytes_impl().await
    }
}

pub struct StreamFrameWriter<W: AsyncWrite + Unpin + Send> {
    peer_id: PeerId,
    writer: W,
}
impl<W: AsyncWrite + Unpin + Send> StreamFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { peer_id: 0, writer }
    }
    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = peer_id;
        self
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> FrameWriter for StreamFrameWriter<W> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    async fn send_frame_impl(&mut self, frame: RpcFrame) -> crate::Result<()> {
        let meta_data = serialize_meta(&frame)?;
        let mut header = Vec::new();
        let mut wr = ChainPackWriter::new(&mut header);
        let msg_len = 1 + meta_data.len() + frame.data().len();
        wr.write_uint_data(msg_len as u64)?;
        header.push(frame.protocol as u8);
        self.writer.write_all(&header).await?;
        self.writer.write_all(&meta_data).await?;
        self.writer.write_all(frame.data()).await?;
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use log::debug;
use shvproto::util::{hex_dump};
use super::*;
    use crate::framerw::test::from_hex;
    use crate::framerw::test::Chunks;
    use crate::RpcMessage;
    use async_std::io::BufWriter;
    use crate::util::hex_string;

    fn init_log() {
        let _ = env_logger::builder()
            //.filter(None, LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }
    async fn send_frame_to_vector(frame: &RpcFrame) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];
        let buffwr = BufWriter::new(&mut buff);
        {
            let mut wr = StreamFrameWriter::new(buffwr);
            wr.send_frame(frame.clone()).await.unwrap();
        }
        buff
    }
    #[async_std::test]
    async fn test_send_frame() {
        init_log();
        for msg in [
            RpcMessage::new_request("foo/bar", "baz1", Some("hello".into())),
            RpcMessage::new_request("foo/bar", "baz2", Some((&[0_u8; 128][..]).into())),
        ] {
            let frame = msg.to_frame().unwrap();
            debug!("frame: {}", &frame);

            let buff = send_frame_to_vector(&frame).await;
            debug!("msg: {msg}");
            debug!("array: {}", hex_string(&buff, Some(" ")));
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
        let data1 = send_frame_to_vector(&msg.to_frame().unwrap()).await;
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
            for _ in 0..1 {
                let frame = rd.receive_frame().await;
                assert!(frame.is_ok());
            }
        }
    }
    #[async_std::test]
    async fn test_read_faulty_frame_by_chunks() {
        init_log();
        for chunks in [
            // <1:1,8:5,9:"foo/bar",10:"baz">i{1:"hello"}
            // invalid protocol
            vec![
                from_hex("21 10 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
        ] {
            let mut rd = StreamFrameReader::new(Chunks { chunks });
            let frame = rd.receive_frame().await;
            debug!("{frame:?}");
            assert!(frame.is_err());
        };
    }
    #[async_std::test]
    async fn test_read_jumbo_frame() {
        init_log();
        for chunks in [
            vec![
                // 140737488355328u ==  0x800000000000
                // 11110010|10000000|00000000|00000000|00000000|00000000|00000000
                // f2 80 00 00 00 00 00
                from_hex("f2 80 00 00 00 00 00 01 8b 41 41 48 45 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 86 05 68 65 6c 6c 6f ff"),
            ],
        ] {
            let mut rd = StreamFrameReader::new(Chunks { chunks });
            let frame = rd.receive_frame().await;
            debug!("{frame:?}");
            assert!(frame.is_err());
        };
    }
}
