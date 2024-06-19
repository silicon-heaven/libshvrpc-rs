use std::io::{BufReader};
use async_trait::async_trait;
use crc::CRC_32_ISO_HDLC;
use crate::rpcframe::{Protocol, RpcFrame};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use log::*;
use shvproto::{ChainPackReader, Reader};
use crate::framerw::{FrameReader, FrameWriter, serialize_meta};

const STX: u8 = 0xA2;
const ETX: u8 = 0xA3;
const ATX: u8 = 0xA4;
const ESC: u8 = 0xAA;

const ESTX: u8 = 0x02;
const EETX: u8 = 0x03;
const EATX: u8 = 0x04;
const EESC: u8 = 0x0A;
pub enum Byte {
    Data(u8),
    Stx,
    Etx,
    Atx,
    FramingError(u8),
}
pub struct SerialFrameReader<R: AsyncRead + Unpin + Send> {
    reader: R,
    with_crc: bool,
}
impl<R: AsyncRead + Unpin + Send> SerialFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            with_crc: false,
        }
    }
    pub fn with_crc_check(mut self, on: bool) -> Self {
        self.with_crc = on;
        self
    }
    async fn get_byte(&mut self) -> crate::Result<u8> {
        let mut buff = [0u8; 1];
        let n = self.reader.read(&mut buff[..]).await?;
        if n == 0 {
            Err("End of stream".into())
        } else {
            Ok(buff[0])
        }
    }
    async fn get_escaped_byte(&mut self) -> crate::Result<crate::serialrw::Byte> {
        match self.get_byte().await? {
            STX => Ok(crate::serialrw::Byte::Stx),
            ETX => Ok(crate::serialrw::Byte::Etx),
            ATX => Ok(crate::serialrw::Byte::Atx),
            ESC => {
                match self.get_byte().await? {
                    ESTX => Ok(crate::serialrw::Byte::Data(STX)),
                    EETX => Ok(crate::serialrw::Byte::Data(ETX)),
                    EATX => Ok(crate::serialrw::Byte::Data(ATX)),
                    EESC => Ok(crate::serialrw::Byte::Data(ESC)),
                    b => {
                        warn!("Framing error, invalid escape byte {}", b);
                        Ok(crate::serialrw::Byte::FramingError(b))
                    }
                }
            }
            b => Ok(crate::serialrw::Byte::Data(b))
        }
    }
    #[cfg(all(test, feature = "async-std"))]
    async fn read_escaped(&mut self) -> crate::Result<Vec<u8>> {
        let mut data: Vec<u8> = Default::default();
        loop {
            match self.get_escaped_byte().await {
                Ok(b) => {
                    match b {
                        Byte::Data(b) => { data.push(b) }
                        Byte::Stx => { data.push( STX) }
                        Byte::Etx => { data.push( ETX ) }
                        Byte::Atx => { data.push( ATX ) }
                        Byte::FramingError(b) => { return Err(format!("Framing error, invalid character {b}").into()) }
                    }
                }
                Err(_) => { break }
            }
        }
        Ok(data)
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for SerialFrameReader<R> {
    async fn receive_frame(&mut self) -> crate::Result<RpcFrame> {
        let mut has_stx = false;
        'read_frame: loop {
            if !has_stx {
                loop {
                    match self.get_escaped_byte().await? {
                        Byte::Stx => { break }
                        _ => { continue }
                    }
                }
            }
            has_stx = false;
            let mut data: Vec<u8> = vec![];
            loop {
                match self.get_escaped_byte().await? {
                    Byte::Stx => {
                        has_stx = true;
                        continue 'read_frame
                    }
                    Byte::Data(b) => { data.push(b) }
                    Byte::Etx => { break }
                    _ => { continue 'read_frame }
                }
            }
            if self.with_crc {
                let mut crc_data = [0u8; 4];
                for crc_b in &mut crc_data {
                    match self.get_escaped_byte().await? {
                        Byte::Stx => {
                            has_stx = true;
                            continue 'read_frame
                        }
                        Byte::Data(b) => { *crc_b = b }
                        _ => { continue 'read_frame }
                    }
                }
                fn as_u32_be(array: &[u8; 4]) -> u32 {
                    ((array[0] as u32) << 24) +
                        ((array[1] as u32) << 16) +
                        ((array[2] as u32) <<  8) +
                        (array[3] as u32)
                }
                let crc1 = as_u32_be(&crc_data);
                let gen = crc::Crc::<u32>::new(&CRC_32_ISO_HDLC);
                let crc2 = gen.checksum(&data);
                //println!("CRC2 {crc2}");
                if crc1 != crc2 {
                    log!(target: "Serial", Level::Debug, "CRC error");
                    continue 'read_frame
                }
            }
            let protocol = data[0];
            if protocol != Protocol::ChainPack as u8 {
                log!(target: "Serial", Level::Debug, "Not chainpack message");
                continue 'read_frame
            }
            let mut buffrd = BufReader::new(&data[1 ..]);
            let mut rd = ChainPackReader::new(&mut buffrd);
            if let Ok(Some(meta)) = rd.try_read_meta() {
                let pos = rd.position() + 1;
                let data: Vec<_> = data.drain(pos .. ).collect();
                let frame  = RpcFrame { protocol: Protocol::ChainPack, meta, data };
                log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
                return Ok(frame)
            } else {
                log!(target: "Serial", Level::Debug, "Meta data read error");
                continue 'read_frame
            }
        }
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
    async fn write_bytes(&mut self, digest: &mut Option<crc::Digest<'_, u32>>, data: &[u8]) -> crate::Result<()> {
        if let Some(ref mut digest) = digest {
            digest.update(data);
        }
        self.writer.write_all(data).await?;
        Ok(())
    }
    async fn write_escaped(&mut self, digest: &mut Option<crc::Digest<'_, u32>>, data: &[u8]) -> crate::Result<()> {
        for b in data {
            match *b {
                STX => { self.write_bytes(digest, &[ESC, ESTX]).await? }
                ETX => { self.write_bytes(digest, &[ESC, EETX]).await? }
                ATX => { self.write_bytes(digest, &[ESC, EATX]).await? }
                ESC => { self.write_bytes(digest, &[ESC, EESC]).await? }
                b => { self.write_bytes(digest, &[b]).await? }
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
            fn u32_to_bytes(x:u32) -> [u8;4] {
                let b0 : u8 = ((x >> 24) & 0xff) as u8;
                let b1 : u8 = ((x >> 16) & 0xff) as u8;
                let b2 : u8 = ((x >> 8) & 0xff) as u8;
                let b3 : u8 = (x & 0xff) as u8;
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
    use async_std::io::BufWriter;
    use crate::RpcMessage;
    use crate::util::{hex_array, hex_dump};
    use super::*;
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
