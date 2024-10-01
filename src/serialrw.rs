use crate::framerw::{read_bytes, FrameData, RawData};
use crate::framerw::{serialize_meta, FrameReader, FrameWriter, ReceiveFrameError};
use crate::rpcframe::{Protocol, RpcFrame};
use async_trait::async_trait;
use crc::{Crc, CRC_32_ISO_HDLC};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use log::*;
use std::mem::{replace};

const STX: u8 = 0xA2;
const ETX: u8 = 0xA3;
const ATX: u8 = 0xA4;
const ESC: u8 = 0xAA;

const ESTX: u8 = 0x02;
const EETX: u8 = 0x03;
const EATX: u8 = 0x04;
const EESC: u8 = 0x0A;
pub enum EscapedByte {
    Data(u8),
    Stx,
    Etx,
    // Atx,
}
fn is_byte_available(raw_data: &RawData) -> bool {
    if raw_data.raw_bytes_available() == 0 {
        false
    } else if raw_data.raw_bytes_available() == 1 {
        raw_data.data[raw_data.consumed] != crate::serialrw::ESC
    } else {
        true
    }
}

pub struct SerialFrameReader<R: AsyncRead + Unpin + Send> {
    reader: R,
    with_crc: bool,
    crc: crc::Digest<'static, u32>,
    has_stx: bool,
    frame_data: FrameData,
    raw_data: RawData,
}
const CRC_32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

impl<R: AsyncRead + Unpin + Send> SerialFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            with_crc: false,
            crc: CRC_32.digest(),
            has_stx: false,
            frame_data: FrameData {
                complete: false,
                meta: None,
                data: vec![],
            },
            raw_data: RawData {
                data: vec![],
                consumed: 0,
            },
        }
    }
    fn reset_frame(&mut self) {
        self.frame_data = FrameData {
            complete: false,
            meta: None,
            data: vec![],
        };
        self.has_stx = false;
        self.crc = CRC_32.digest();
        self.raw_data.trim();
    }
    pub fn with_crc_check(mut self, on: bool) -> Self {
        self.with_crc = on;
        self
    }
    async fn get_raw_byte(&mut self) -> Result<u8, ReceiveFrameError> {
        if self.raw_data.raw_bytes_available() == 0 {
            read_bytes(&mut self.reader, &mut self.raw_data.data).await?;
        }
        let b = self.raw_data.data[self.raw_data.consumed];
        if self.has_stx && !self.frame_data.complete {
            self.crc.update(&[b]);
        }
        self.raw_data.consumed += 1;
        Ok(b)
    }
    fn unget_raw_byte(&mut self) {
        assert!(!self.raw_data.data.is_empty());
        self.raw_data.consumed -= 1;
    }
    fn unescape_byte(b: u8) -> Result<u8, ReceiveFrameError> {
        let b = match b {
            ESTX => STX,
            EETX => ETX,
            EATX => ATX,
            EESC => ESC,
            b => {
                warn!("Framing error, invalid escape byte {}", b);
                return Err(ReceiveFrameError::FrameError);
            }
        };
        Ok(b)
    }
    async fn get_frame_data_byte(&mut self) -> Result<(), ReceiveFrameError> {
        if !self.has_stx {
            loop {
                if self.get_raw_byte().await? == STX {
                    self.reset_frame();
                    break;
                }
            }
        }
        match self.get_raw_byte().await? {
            STX => {
                self.unget_raw_byte();
                return Err(ReceiveFrameError::FrameError);
            }
            ETX => {
                if self.with_crc {
                    let mut crc_data = [0u8; 4];
                    for crc_b in &mut crc_data {
                        let b = match self.get_raw_byte().await? {
                            STX => {
                                self.unget_raw_byte();
                                return Err(ReceiveFrameError::FrameError);
                            }
                            ESC => {
                                Self::unescape_byte(self.get_raw_byte().await?)?
                            }
                            ATX => {
                                return Err(ReceiveFrameError::FrameError);
                            }
                            ETX => {
                                return Err(ReceiveFrameError::FrameError);
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
                    let digest = replace(&mut self.crc, CRC_32.digest());
                    let crc2 = digest.finalize();
                    //println!("CRC2 {crc2}");
                    if crc1 != crc2 {
                        log!(target: "Serial", Level::Debug, "CRC error");
                        return Err(ReceiveFrameError::FrameError)
                    }
                }
                self.frame_data.complete = true;
                return Ok(())
            }
            ATX => return Err(ReceiveFrameError::FrameError),
            ESC => {
                let ub = Self::unescape_byte(self.get_raw_byte().await?)?;
                self.frame_data.data.push(ub)
            },
            b => self.frame_data.data.push(b),
        };
        Ok(())
    }
    #[cfg(all(test, feature = "async-std"))]
    async fn read_escaped(&mut self) -> crate::Result<Vec<u8>> {
        let mut data: Vec<u8> = Default::default();
        while let Ok(b) = self.get_raw_byte().await {
            let b = match b {
                ESC => Self::unescape_byte(self.get_raw_byte().await?)?,
                b => b,
            };
            data.push(b);
        }
        Ok(data)
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for SerialFrameReader<R> {
    async fn get_byte(&mut self) -> Result<(), ReceiveFrameError> {
        self.get_frame_data_byte().await
    }
    fn can_read_meta(&self) -> bool {
        self.frame_data.complete || !is_byte_available(&self.raw_data)
    }
    fn frame_data_ref_mut(&mut self) -> &mut FrameData {
        &mut self.frame_data
    }

    fn reset_frame_data(&mut self) {
        self.reset_frame()
    }
}

pub struct SerialFrameWriter<W: AsyncWrite + Unpin + Send> {
    writer: W,
    with_crc: bool,
}
impl<W: AsyncWrite + Unpin + Send> SerialFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
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
        if let Some(ref mut digest) = digest {
            digest.update(data);
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
    async fn send_frame(&mut self, frame: RpcFrame) -> crate::Result<()> {
        log!(target: "RpcMsg", Level::Debug, "S<== {}", &frame);
        let gen = crc::Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let mut digest = if self.with_crc {
            Some(gen.digest())
        } else {
            None
        };
        let meta_data = serialize_meta(&frame)?;
        self.writer.write_all(&[STX]).await?;
        let protocol = [Protocol::ChainPack as u8];
        self.write_escaped(&mut digest, &protocol).await?;
        self.write_escaped(&mut digest, &meta_data).await?;
        self.write_escaped(&mut digest, &frame.data).await?;
        self.writer.write_all(&[ETX]).await?;
        if self.with_crc {
            fn u32_to_bytes(x: u32) -> [u8; 4] {
                let b0: u8 = ((x >> 24) & 0xff) as u8;
                let b1: u8 = ((x >> 16) & 0xff) as u8;
                let b2: u8 = ((x >> 8) & 0xff) as u8;
                let b3: u8 = (x & 0xff) as u8;
                [b0, b1, b2, b3]
            }
            let crc = digest.expect("digest should be some here").finalize();
            //println!("CRC1 {crc}");
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

#[cfg(all(test, feature = "async-std"))]
mod test {
    use super::*;
    use crate::util::{hex_array, hex_dump};
    use crate::RpcMessage;
    use async_std::io::BufWriter;
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
    async fn test_write_frame() {
        let msg = RpcMessage::new_request("foo/bar", "baz", Some("hello".into()));
        for with_crc in [false, true] {
            let frame = msg.to_frame().unwrap();
            let mut buff: Vec<u8> = vec![];
            let buffwr = BufWriter::new(&mut buff);
            {
                let mut wr = SerialFrameWriter::new(buffwr).with_crc_check(with_crc);
                wr.send_frame(frame.clone()).await.unwrap();
            }
            debug!("msg: {}", msg);
            debug!("array: {}", hex_array(&buff));
            debug!("bytes:\n{}\n-------------", hex_dump(&buff));
            for prefix in [
                b"".to_vec(),
                b"1234".to_vec(),
                [STX].to_vec(),
                [STX, ESC, 1u8].to_vec(),
                [ATX].to_vec(),
            ] {
                let mut buff2: Vec<u8> = prefix;
                buff2.append(&mut buff.clone());
                let buffrd = async_std::io::BufReader::new(&*buff2);
                let mut rd = SerialFrameReader::new(buffrd).with_crc_check(with_crc);
                let rd_frame = rd.receive_frame().await.unwrap();
                assert_eq!(&rd_frame, &frame);
            }
        }
    }
}
