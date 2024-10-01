use std::io::{BufReader};
use async_trait::async_trait;
use crate::rpcframe::{Protocol, RpcFrame};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use log::*;
use shvproto::{ChainPackReader, ChainPackWriter, Reader, ReadError};
use crate::framerw::{FrameReader, FrameWriter, serialize_meta, ReceiveFrameError, read_bytes, RawData, FrameData};
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
            let proto = self.get_raw_byte().await?;
            if proto == Protocol::ResetSession as u8 {
                return Err(ReceiveFrameError::StreamError);
            }
            if proto != Protocol::ChainPack as u8 {
                return Err(ReceiveFrameError::FrameError);
            }
            self.frame_len = frame_len;
        }
        if self.frame_data.data.len() == self.frame_len {
            self.frame_data.complete = true;
        }
        Ok(())
    }
}
#[async_trait]
impl<R: AsyncRead + Unpin + Send> FrameReader for StreamFrameReader<R> {
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
    // async fn try_receive_frame(&mut self) -> Result<RpcFrameReception, ReceiveFrameError> {
    //     let make_error = |err: ReceiveFrameError| {
    //         self.reset_frame(None);
    //         Err(err)
    //     };
    //     loop {
    //         if self.frame_len == 0 {
    //             let mut lendata: Vec<u8> = vec![];
    //             let frame_len = loop {
    //                 lendata.push(self.get_byte().await?);
    //                 let mut buffrd = BufReader::new(&lendata[..]);
    //                 let mut rd = ChainPackReader::new(&mut buffrd);
    //                 match rd.read_uint_data() {
    //                     Ok(len) => { break len as usize }
    //                     Err(err) => {
    //                         let msg = err.to_string();
    //                         let ReadError{reason, .. } = err;
    //                         match reason {
    //                             ReadErrorReason::UnexpectedEndOfStream => {
    //
    //                             }
    //                             ReadErrorReason::InvalidCharacter => {
    //
    //                             }
    //                         }
    //                     }
    //                 };
    //             };
    //             let proto = self.get_byte().await?;
    //             if proto == Protocol::ResetSession as u8 {
    //                 return make_error(ReceiveFrameError::StreamError);
    //             }
    //             if proto != Protocol::ChainPack as u8 {
    //                 return make_error(ReceiveFrameError::FrameError);
    //             }
    //             self.frame_len = frame_len;
    //         } else if self.data.len() < self.frame_len {
    //             self.get_byte().await?;
    //         }
    //         if self.meta.is_none() && (self.raw_data.raw_bytes_available() == 0 || self.data.len() == self.frame_len) {
    //             let mut buffrd = BufReader::new(&self.data[1..]);
    //             let mut rd = ChainPackReader::new(&mut buffrd);
    //             if let Ok(Some(meta)) = rd.try_read_meta() {
    //                 let pos = rd.position() + 1;
    //                 self.data.drain(..pos);
    //                 let resp_id = if meta.is_response() {
    //                     meta.request_id()
    //                 } else {
    //                     None
    //                 };
    //                 self.meta = Some(meta);
    //                 if let Some(rqid) = resp_id {
    //                     return Ok(RpcFrameReception::ResponseId(rqid));
    //                 }
    //             } else {
    //                 log!(target: "Serial", Level::Debug, "Meta data read error");
    //                 return make_error(ReceiveFrameError::FrameError);
    //             }
    //         }
    //         if self.data.len() == self.frame_len && self.meta.is_some() {
    //             let frame = RpcFrame {
    //                 protocol: Protocol::ChainPack,
    //                 meta: take(&mut self.meta).unwrap(),
    //                 data: take(&mut self.data),
    //             };
    //             log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
    //             return Ok(RpcFrameReception::Frame(frame))
    //         }
    //     }
    // }
}

pub fn read_frame(buff: &[u8]) -> crate::Result<RpcFrame> {
    // log!(target: "RpcData", Level::Debug, "\n{}", hex_dump(buff));
    let mut buffrd = BufReader::new(buff);
    let mut rd = ChainPackReader::new(&mut buffrd);
    let frame_len = match rd.read_uint_data() {
        Ok(len) => { len as usize }
        Err(err) => {
            return Err(err.msg.into());
        }
    };
    let pos = rd.position();
    let data = &buff[pos .. pos + frame_len];
    let protocol = if data[0] == 0 {Protocol::ResetSession} else { Protocol::ChainPack };
    let data = &data[1 .. ];
    let mut buffrd = BufReader::new(data);
    let mut rd = ChainPackReader::new(&mut buffrd);
    if let Ok(Some(meta)) = rd.try_read_meta() {
        let pos = rd.position();
        let frame = RpcFrame { protocol, meta, data: data[pos ..].to_vec() };
        log!(target: "RpcMsg", Level::Debug, "R==> {}", &frame);
        return Ok(frame);
    }
    Err("Meta data read error".into())
}

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

pub fn write_frame(buff: &mut Vec<u8>, frame: RpcFrame) -> crate::Result<()> {
    let mut meta_data = serialize_meta(&frame)?;
    let mut header = Vec::new();
    let mut wr = ChainPackWriter::new(&mut header);
    let msg_len = 1 + meta_data.len() + frame.data.len();
    wr.write_uint_data(msg_len as u64)?;
    header.push(frame.protocol as u8);
    let mut frame = frame;
    buff.append(&mut header);
    buff.append(&mut meta_data);
    buff.append(&mut frame.data);
    Ok(())
}


