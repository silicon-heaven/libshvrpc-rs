use crate::framerw::{
    format_peer_id, read_raw_data, FrameReader, FrameReaderPrivate, RawData,
};
use crate::framerw::{serialize_meta, FrameWriter, ReceiveFrameError};
use crate::rpcframe::{RpcFrame};
use crate::rpcmessage::PeerId;
use async_trait::async_trait;
use crc::{Crc, Digest, CRC_32_ISO_HDLC};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use log::*;
use shvproto::util::hex_dump;

const STX: u8 = 0xA2;
const ETX: u8 = 0xA3;
const ATX: u8 = 0xA4;
const ESC: u8 = 0xAA;

const ESTX: u8 = 0x02;
const EETX: u8 = 0x03;
const EATX: u8 = 0x04;
const EESC: u8 = 0x0A;

pub struct SerialFrameReader<R: AsyncRead + Unpin + Send> {
    peer_id: PeerId,
    reader: R,
    with_crc: bool,
    // crc_digest: crc::Digest<'static, u32>,
    raw_data: RawData,
}

// https://reveng.sourceforge.io/crc-catalogue/all.htm#crc.cat.crc-32-iso-hdlc
// https://crccalc.com
const CRC_32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

impl<R: AsyncRead + Unpin + Send> SerialFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            peer_id: 0,
            reader,
            with_crc: false,
            // crc_digest: CRC_32.digest(),
            raw_data: RawData::new(),
        }
    }
    pub fn with_crc_check(mut self, on: bool) -> Self {
        self.with_crc = on;
        self
    }
    async fn get_raw_byte(&mut self, with_timeout: bool) -> Result<u8, ReceiveFrameError> {
        if self.raw_data.bytes_available() == 0 {
            read_raw_data(&mut self.reader, &mut self.raw_data, with_timeout).await?;
        }
        let b = self.raw_data.data[self.raw_data.consumed];
        self.raw_data.consumed += 1;
        Ok(b)
    }
    fn unget_stx(&mut self) {
        self.raw_data.consumed -= 1;
        assert!(self.raw_data.data.len() > self.raw_data.consumed);
        assert_eq!(self.raw_data.data[self.raw_data.consumed], STX);
    }
    fn unescape_byte(b: u8) -> Result<u8, ReceiveFrameError> {
        let b = match b {
            ESTX => STX,
            EETX => ETX,
            EATX => ATX,
            EESC => ESC,
            b => {
                return Err(ReceiveFrameError::FramingError(format!(
                    "Framing error, invalid escape byte {:#02x}",
                    b
                )));
            }
        };
        Ok(b)
    }
    async fn get_frame_bytes_impl(&mut self) -> Result<Vec<u8>, ReceiveFrameError> {
        let mut crc_digest = if self.with_crc { Some(CRC_32.digest()) } else { None };
        let update_crc_digest = |crc_digest: &mut Option<Digest<u32>>, b: u8| {
            if let Some(crc_digest) = crc_digest {
                crc_digest.update(&[b]);
            }
        };

        let mut data = vec![];
        while self.get_raw_byte(false).await? != STX {}
        loop {
            match self.get_raw_byte(true).await? {
                STX => {
                    self.unget_stx();
                    return Err(ReceiveFrameError::FramingError(
                        "Incomplete frame, new STX received within data.".into(),
                    ));
                }
                ETX => {
                    if let Some(crc_digest) = crc_digest {
                        let mut crc_data = [0u8; 4];
                        for crc_b in &mut crc_data {
                            let b = match self.get_raw_byte(true).await? {
                                STX => {
                                    self.unget_stx();
                                    return Err(ReceiveFrameError::FramingError(
                                        "STX received in CRC.".into(),
                                    ));
                                }
                                ESC => Self::unescape_byte(self.get_raw_byte(true).await?)?,
                                ATX => {
                                    return Err(ReceiveFrameError::FramingError(
                                        "ATX received in CRC.".into(),
                                    ));
                                }
                                ETX => {
                                    return Err(ReceiveFrameError::FramingError(
                                        "ETX received in CRC.".into(),
                                    ));
                                }
                                b => b,
                            };
                            *crc_b = b;
                        }
                        fn as_u32_be(array: &[u8; 4]) -> u32 {
                            ((array[0] as u32) << 24)
                                + ((array[1] as u32) << 16)
                                + ((array[2] as u32) << 8)
                                + (array[3] as u32)
                        }
                        let crc1 = as_u32_be(&crc_data);
                        // let digest = replace(&mut self.crc_digest, CRC_32.digest());
                        let crc2 = crc_digest.finalize();
                        //info!("CRC1 {:#04x}", crc1);
                        //info!("CRC2 {:#04x}", crc2);
                        if crc1 != crc2 {
                            log!(target: "Serial", Level::Warn, "CRC error");
                            return Err(ReceiveFrameError::FramingError("CRC check error.".into()));
                        }
                    }
                    break;
                }
                ATX => {
                    return Err(ReceiveFrameError::FramingError(
                        "ATX received, aborting current frame.".into(),
                    ))
                }
                ESC => {
                    update_crc_digest(&mut crc_digest, ESC);
                    let b = self.get_raw_byte(true).await?;
                    if b == STX {
                        self.unget_stx();
                        return Err(ReceiveFrameError::FramingError(
                            "Incomplete frame, new STX received within escaped data.".into(),
                        ));
                    }
                    update_crc_digest(&mut crc_digest, b);
                    let ub = Self::unescape_byte(b)?;
                    data.push(ub)
                }
                b => {
                    update_crc_digest(&mut crc_digest, b);
                    data.push(b)
                }
            };
        }
        Ok(data)
    }
    #[cfg(test)]
    async fn read_escaped(&mut self) -> crate::Result<Vec<u8>> {
        let mut data: Vec<u8> = Default::default();
        while let Ok(b) = self.get_raw_byte(false).await {
            let b = match b {
                ESC => Self::unescape_byte(self.get_raw_byte(false).await?)?,
                b => b,
            };
            data.push(b);
        }
        Ok(data)
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReaderPrivate for SerialFrameReader<R> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    async fn get_frame_bytes(&mut self) -> Result<Vec<u8>, ReceiveFrameError> {
        self.get_frame_bytes_impl().await
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for SerialFrameReader<R> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    fn set_peer_id(&mut self, peer_id: PeerId) {
        self.peer_id = peer_id
    }
    async fn receive_frame(&mut self) -> Result<RpcFrame, ReceiveFrameError> {
        loop {
            match self.receive_frame_private().await {
                Ok(frame) => return Ok(frame),
                Err(ReceiveFrameError::FramingError(e)) => {
                    // silently ignore ATX, and CRC erorrs
                    log!(target: "SerialFrameError", Level::Warn, "Ignoring serial framing error: {}", e);
                    continue;
                }
                Err(e) => {
                    return Err(e)
                }
            }
        }
    }
}
pub struct SerialFrameWriter<W: AsyncWrite + Unpin + Send> {
    peer_id: PeerId,
    writer: W,
    with_crc: bool,
}
impl<W: AsyncWrite + Unpin + Send> SerialFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            peer_id: 0,
            writer,
            with_crc: false,
        }
    }
    pub fn with_crc_check(mut self, on: bool) -> Self {
        self.with_crc = on;
        self
    }
    async fn write_bytes(
        &mut self,
        digest: &mut Option<crc::Digest<'_, u32>>,
        data: &[u8],
    ) -> crate::Result<()> {
        if let Some(digest) = digest {
            digest.update(data);
        }
        if log_enabled!(target: "RpcData", Level::Debug) {
            log!(target: "RpcData", Level::Debug, "data sent --> {}", hex_dump(data));
        }
        self.writer.write_all(data).await?;
        Ok(())
    }
    async fn write_escaped(
        &mut self,
        digest: &mut Option<crc::Digest<'_, u32>>,
        data: &[u8],
    ) -> crate::Result<()> {
        for b in data {
            match *b {
                STX => self.write_bytes(digest, &[ESC, ESTX]).await?,
                ETX => self.write_bytes(digest, &[ESC, EETX]).await?,
                ATX => self.write_bytes(digest, &[ESC, EATX]).await?,
                ESC => self.write_bytes(digest, &[ESC, EESC]).await?,
                b => self.write_bytes(digest, &[b]).await?,
            };
        }
        Ok(())
    }
}
#[async_trait]
impl<W: AsyncWrite + Unpin + Send> FrameWriter for SerialFrameWriter<W> {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }
    fn set_peer_id(&mut self, peer_id: PeerId) {
        self.peer_id = peer_id
    }
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()> {
        log!(target: "RpcMsg", Level::Debug, "S<== {} {}", format_peer_id(self.peer_id), &frame);
        let crc = crc::Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let mut digest = if self.with_crc {
            Some(crc.digest())
        } else {
            None
        };
        let meta_data = serialize_meta(&frame)?;
        self.write_bytes(&mut None, &[STX]).await?;
        let protocol = [frame.protocol as u8];
        self.write_escaped(&mut digest, &protocol).await?;
        self.write_escaped(&mut digest, &meta_data).await?;
        self.write_escaped(&mut digest, &frame.data).await?;
        self.write_bytes(&mut None, &[ETX]).await?;
        if self.with_crc {
            fn u32_to_bytes(x: u32) -> [u8; 4] {
                let b0: u8 = ((x >> 24) & 0xff) as u8;
                let b1: u8 = ((x >> 16) & 0xff) as u8;
                let b2: u8 = ((x >> 8) & 0xff) as u8;
                let b3: u8 = (x & 0xff) as u8;
                [b0, b1, b2, b3]
            }
            let crc = digest.expect("digest should be some here").finalize();
            let crc_bytes = u32_to_bytes(crc);
            self.write_escaped(&mut None, &crc_bytes).await?;
        }
        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::framerw::test::from_hex;
    use crate::util::{hex_string};
    use crate::RpcMessage;
    use async_std::io::BufWriter;
    use crate::rpcframe::Protocol;

    fn init_log() {
        let _ = env_logger::builder()
            // .filter(None, LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }
    #[async_std::test]
    async fn test_write_bytes() {
        for (data, esc_data) in [
            (&b"hello"[..], &b"hello"[..]),
            (&[STX], &[ESC, ESTX]),
            (&[ETX], &[ESC, EETX]),
            (&[ATX], &[ESC, EATX]),
            (&[ESC], &[ESC, EESC]),
            (&[STX, ESC], &[ESC, ESTX, ESC, EESC]),
        ] {
            let mut buff: Vec<u8> = vec![];
            {
                let buffwr = BufWriter::new(&mut buff);
                let mut wr = SerialFrameWriter::new(buffwr);
                wr.write_escaped(&mut None, data).await.unwrap();
                //drop(wr);
                wr.writer.flush().await.unwrap();
                assert_eq!(&buff, esc_data);
            }
            {
                let buffrd = async_std::io::BufReader::new(&*buff);
                let mut rd = SerialFrameReader::new(buffrd);
                let read_data = rd.read_escaped().await.unwrap();
                assert_eq!(&read_data, data);
            }
        }
    }

    #[async_std::test]
    async fn test_write_reset_session_frame() {
        init_log();
        let reset_frame_data = [STX, Protocol::ResetSession as u8, ETX];
        {
            let frame = RpcFrame::new_reset_session();
            let mut buff: Vec<u8> = vec![];
            let buffwr = BufWriter::new(&mut buff);
            {
                let mut wr = SerialFrameWriter::new(buffwr).with_crc_check(false);
                wr.send_frame(frame.clone()).await.unwrap();
            }
            assert_eq!(&buff[..], reset_frame_data);
        }
        {
            let mut crc_digest = CRC_32.digest();
            crc_digest.update(&reset_frame_data[1 .. 2]);
            let crc = crc_digest.finalize();
            assert_eq!(crc, 0xd202ef8d);
            let frame = RpcFrame::new_reset_session();
            let mut buff: Vec<u8> = vec![];
            let buffwr = BufWriter::new(&mut buff);
            {
                let mut wr = SerialFrameWriter::new(buffwr).with_crc_check(true);
                wr.send_frame(frame.clone()).await.unwrap();
            }
            assert_eq!(&buff[0 .. reset_frame_data.len()], reset_frame_data);
            assert_eq!(&buff[reset_frame_data.len() ..], [0xd2, 0x02, 0xef, 0x8d]);
        }
    }

    #[async_std::test]
    async fn test_write_read_frame() {
        init_log();

        for with_crc in [false, true] {
            let msg = RpcMessage::new_request("foo/bar", "baz", Some(with_crc.into()));
            let frame = msg.to_frame().unwrap();
            let mut buff: Vec<u8> = vec![];
            let buffwr = BufWriter::new(&mut buff);
            {
                let mut wr = SerialFrameWriter::new(buffwr).with_crc_check(with_crc);
                wr.send_frame(frame.clone()).await.unwrap();
            }
            debug!("msg: {}", msg);
            debug!("len: {}, hex: {}", buff.len(), hex_string(&buff, Some(" ")));
            debug!("with crc: {with_crc}");
            for prefix in [b"".to_vec(), b"1234".to_vec(), [ATX].to_vec()] {
                let mut buff2 = prefix;
                buff2.append(&mut buff.clone());
                //debug!("bytes:\n{}\n-------------", hex_dump(&buff2));
                let buffrd = async_std::io::BufReader::new(&*buff2);
                let mut rd = SerialFrameReader::new(buffrd).with_crc_check(with_crc);
                let rd_frame = rd.receive_frame().await.unwrap();
                assert_eq!(&rd_frame, &frame);
            }
            for prefix in [
                [STX].to_vec(),
                [STX, ESC].to_vec(),
                [STX, ESC, 1u8].to_vec(),
                [STX, ATX].to_vec(),
                [STX, ESC, ATX].to_vec(),
            ] {
                let mut buff2 = prefix;
                buff2.append(&mut buff.clone());
                //debug!("bytes:\n{}\n-------------", hex_dump(&buff2));
                let buffrd = async_std::io::BufReader::new(&*buff2);
                let mut rd = SerialFrameReader::new(buffrd).with_crc_check(with_crc);
                let rd_frame = rd.receive_frame().await.unwrap();
                assert_eq!(&rd_frame, &frame);
            }
        }
    }

    #[async_std::test]
    async fn test_read_frame_by_chunks() {
        init_log();

        for chunks in [
            // msg: <1:1,8:1,9:"foo/bar",10:"baz">i{1:1}
            vec![
                from_hex("STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df"),
            ],
            vec![
                from_hex("STX 01 8b 41 41"),
                from_hex("48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX"),
                from_hex("44 cc 24 df"),
            ],

            // ESC in CRC
            // msg: <1:1,8:19,9:"foo/bar",10:"baz">i{1:18}
            vec![
                from_hex("STX 01 8b 41 41 48 53 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 52 ff ETX ESC ESTX 84 e6 8c"),
            ],
            vec![
                from_hex("STX 01 8b 41 41 48 53 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 52 ff ETX ESC"),
                from_hex("ESTX 84 e6 8c"),
            ],

            // ESC in data
            // msg: <1:1,8:162,9:"foo/bar",10:"baz">i{1:161}
            vec![
                from_hex("STX 01 8b 41 41 48 82 80 ESC ESTX 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 82 80 a1 ff ETX 12 53 57 e5"),
            ],
            vec![
                from_hex("STX 01 8b 41 41 48 82 80 ESC"),
                from_hex("02 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 82 80 a1 ff"),
                from_hex("ETX"),
                from_hex("12 53 57"),
                from_hex("e5"),
            ],
        ] {
            debug!("hex: {:?}", chunks);
            let mut rd = SerialFrameReader::new(crate::framerw::test::Chunks { chunks }).with_crc_check(true);
            let frame = rd.receive_frame().await;
            debug!("frame: {:?}", &frame);
            assert!(frame.is_ok());
        };
    }

    #[async_std::test]
    async fn test_read_multiframe_chunk() {
        init_log();
        for with_crc in [false, true] {
            for data in [
                // msg: <1:1,8:1,9:"foo/bar",10:"baz">i{1:1}
                from_hex(
                    "STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df
                    STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df
                    STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df
                    STX 01 8b 41 41 48 41 49 86 07 66 6f 6f "
                ),
                from_hex(
                    "STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df
                     01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df
                 STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df
                 ESC 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff     44 cc 24 df
                 STX 01 8b 41 41 48 41 49 86 07 66 6f 6f 2f 62 61 72 4a 86 03 62 61 7a ff 8a 41 41 ff ETX 44 cc 24 df"
                ),
            ] {
                debug!("bytes:\n{}\n-------------", hex_dump(&data));
                let buffrd = async_std::io::BufReader::new(&*data);
                let mut rd = SerialFrameReader::new(buffrd).with_crc_check(with_crc);
                for _ in 0 .. 3 {
                    let frame = rd.receive_frame().await;
                    debug!("frame: {:?}", &frame);
                    assert!(frame.is_ok());
                }
            }
        }
    }
}
