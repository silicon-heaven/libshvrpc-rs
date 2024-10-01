use std::collections::VecDeque;
use std::io::{BufReader};
use async_trait::async_trait;
use crc::CRC_32_ISO_HDLC;
use crate::rpcframe::{Protocol, RpcFrame};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use log::*;
use shvproto::{ChainPackReader, MetaMap, Reader};
use crate::framerw::{FrameReader, FrameWriter, serialize_meta, FrameOrResponseId, ReadFrameError, SomeData};
use crate::rpcmessage::RqId;
use crate::RpcMessageMetaTags;

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
enum ReadFrameResult {
    Frame(RpcFrame),
    ResponseId(RqId),
    FrameError,
    StreamError,
}
enum ReadBytesResult {
    Ok,
    Timeout,
    FrameError,
    StreamError,
}
struct RawData {
    data: Vec<u8>,
    in_escape: bool,
    in_frame: bool,
}
struct FrameData {
    data: Vec<u8>,
    meta: Option<MetaMap>,
    data_start: usize,
    completed: bool,
}
pub struct SerialFrameReader<R: AsyncRead + Unpin + Send> {
    reader: R,
    with_crc: bool,
    frame_data: FrameData,
    raw_data: RawData,
}
impl<R: AsyncRead + Unpin + Send> SerialFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            with_crc: false,
            // state: ReaderState::WaitingForStx,
            // meta: None,
            // data_start: 0,
            frame_data: FrameData {
                data: vec![],
                meta: Default::default(),
                data_start: 0,
                completed: false,
            },
            raw_data: RawData {
                data: vec![],
                in_escape: false,
                in_frame: false,
            },
        }
    }
    pub fn with_crc_check(mut self, on: bool) -> Self {
        self.with_crc = on;
        self
    }
    // async fn get_escaped_bytes(&mut self) -> ReadBytesResult {
    //     const BUFF_LEN: usize = 1024 * 4;
    //     let mut buff = [0; BUFF_LEN];
    //     let n = self.reader.read(&mut buff).await?;
    //     if n == 0 {
    //         ReadBytesResult::StreamError
    //     } else {
    //
    //         for b in &buff[0 .. n] {
    //             if self.last_byte_read == ESC {
    //                 match b {
    //                     ESTX => self.frames.push(STX),
    //                     EETX => self.frames.push(ETX),
    //                     EATX => self.frames.push(ATX),
    //                     EESC => self.frames.push(ESC),
    //                     b => {
    //                         return Err(format!("Framing error, invalid escape byte {}", b).into());
    //                     }
    //                 }
    //             } else {
    //                 self.frames.push(*b);
    //             };
    //             self.last_byte_read = *b;
    //         }
    //         Ok(())
    //     }
    // }
    async fn read_bytes(&mut self) -> Result<(), ReadFrameError> {
        const BUFF_LEN: usize = 1024 * 4;
        let mut buff = [0; BUFF_LEN];
        let n = match self.reader.read(&mut buff).await {
            Ok(n) => { n }
            Err(_) => {
                return Err(crate::framerw::ReadFrameError::StreamError)
            }
        };
        if n == 0 {
            Err(crate::framerw::ReadFrameError::StreamError)
        } else {
            self.raw_data.data.append(&mut buff.to_vec());
            Ok(())
        }
    }
    async fn get_some_data(&mut self) -> Result<SomeData, ReadFrameError>  {
        let mut ret = SomeData { data: vec![], first: false, last: false };
        if !self.raw_data.in_frame {
            // wait for STX
            loop {
                if let Some(pos) = self.raw_data.data.iter().position(STX) {
                    self.raw_data.data.drain(0 .. pos + 1);
                    self.raw_data.in_frame = true;
                    ret.first = true;
                    break;
                }
                self.read_bytes().await?;
            }
        }
        let mut consumed: usize = 0;
        let mut recent_b: u8 = 0;
        for b in &self.raw_data.data[ .. ] {
            consumed += 1;
            match b {
                STX => {
                    ret = SomeData { data: vec![], first: false, last: false };
                },
                ATX => {
                    // cannot simply skip, because meta might be returned already
                    return Err(ReadFrameError::FrameError);
                },
                ETX => {
                    // check CRC
                    if self.with_crc {
                        
                    }
                },
                b => {
                    if recent_b == ESC {
                        match b {
                            ESTX => ret.data.push(STX),
                            EETX => ret.data.push(ETX),
                            EATX => ret.data.push(ATX),
                            EESC => ret.data.push(ESC),
                            b => {
                                return Err(ReadFrameError::FrameError);
                            }
                        }
                        consumed += 2;
                    } else if b == ESC {
                        consumed -= 1;
                    } else {
                        ret.data.push(*b);
                    };
                }

            }
            recent_b = *b;
        }
        if self.raw_data.is_empty() || self.raw_data.len() == 1 && self.raw_data[0] == ESC {
            self.read_bytes().await?;
        }
        Ok(())
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for SerialFrameReader<R> {
    async fn receive_frame_or_response_id(&mut self) -> Result<FrameOrResponseId, ReadFrameError>  {
        loop {
            self.get_some_data().await?;
            if self.frame_data.meta.is_none() {
                // read protocol
                if self.frame_data.data.len() > 0 {
                    match self.frame_data.data[0] {
                        0 => {
                            // Protocol::ResetSession
                            return Err(ReadFrameError::FrameError);
                        }
                        1 => {
                            //Protocol::ChainPack
                        },
                        _ => {
                            // Protocol not supported
                            return Err(ReadFrameError::FrameError);
                        }
                    };
                }
                let mut buffrd = BufReader::new(&self.frame_data.data[1 ..]);
                let mut rd = ChainPackReader::new(&mut buffrd);
                if let Ok(Some(meta)) = rd.try_read_meta() {
                    self.frame_data.data_start = rd.position() + 2;
                    self.frame_data.meta = Some(meta);
                    let meta = self.frame_data.meta.as_ref().unwrap();
                    return if meta.is_response() {
                        Ok(FrameOrResponseId::ResponseId(meta.request_id().unwrap_or_default()))
                    } else {
                        Ok(FrameOrResponseId::ResponseId(0))
                    }
                    //log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
                }
            } else {
                if self.frame_data.completed {
                    let mut data = std::mem::take(&mut self.frame_data.data);
                    data.drain(.. self.frame_data.data_start);
                    let frame = RpcFrame {
                        protocol: Protocol::ChainPack,
                        meta: std::mem::take(&mut self.frame_data.meta),
                        data,
                    };
                    Ok(FrameOrResponseId::Frame(frame))
                }
            }
        }
        self.get_escaped_bytes().await?;
        if self.frames[0] != STX {
            if let Some(n) = self.frames.iter().position(STX) {
                self.frames.drain(0 .. n);
                self.state = ReaderState::WaitingForEtx;

            } else {
                self.frames = vec![];
            }
        }
        if self.frames.len() > 1 && self.frames[0] == STX {
            let data = self.frames[1 ..];
            let protocol = match self.frames[0] {
                0 => Protocol::ResetSession,
                1 => Protocol::ChainPack,
                _ => {
                    self.frames.drain(0 .. 2);
                    return Err("Invalid protocol received".into());
                }
            };
            if protocol == Protocol::ResetSession {
                self.frames.drain(0 .. 2);
                return Err("Session reset".into());
            }
            if self.meta.is_none() {
                let mut buffrd = BufReader::new(&self.frames[1 ..]);
                let mut rd = ChainPackReader::new(&mut buffrd);
                if let Ok(Some(meta)) = rd.try_read_meta() {
                    self.data_start = rd.position() + 2;
                    self.meta = Some(meta);
                    let meta = self.meta.as_ref().unwrap();
                    if meta.is_response() {
                        return Ok(FrameOrResponseId::ResponseId(meta.request_id().unwrap_or_default()));
                    }
                    //log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
                }
            }
        }
        match self.state {
            ReaderState::WaitingForStx => {
            }
            ReaderState::WaitingForEtx => {}
            ReaderState::WaitingForCrc => {}
        }
        'read_frame: loop {
            if !self.has_stx {
                loop {
                    match self.get_escaped_byte().await? {
                        Byte::Stx => { break }
                        _ => { continue }
                    }
                }
            }
            self.has_stx = false;
            let mut data: Vec<u8> = vec![];
            loop {
                match self.get_escaped_byte().await? {
                    Byte::Stx => {
                        self.has_stx = true;
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
                            self.has_stx = true;
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
