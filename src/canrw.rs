// SHV RPC over CAN-FD
// https://silicon-heaven.github.io/shv-doc/rpctransportlayer/can.html
//
// Data frames are represented using CAN FD frames (CanFdFrame).
// Remote frames (RTR) are represented using classic CAN frames (CanFrame).
// CanFrameReader/Writer assembles multi-frame messages using the 7-bit counter
// and generates acknowledgements as CAN FD frames.

use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream, StreamExt};
use futures_time::time::Duration;
use socketcan::id::FdFlags;
use socketcan::{CanFdFrame, CanId, CanRemoteFrame, EmbeddedFrame, Frame};
use thiserror::Error;

use crate::framerw::{serialize_meta, FrameReader, FrameWriter, ReceiveFrameError};
use crate::rpcmessage::PeerId;
use crate::streamrw::DEFAULT_FRAME_SIZE_LIMIT;
use crate::RpcFrame;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtrKind {
    AddressAcquisition,
    AddressAnnounceAccept,
    AddressAnnounceReject,
    DiscoveryAccept,
    DiscoveryReject,
    DiscoveryAll,
    Other(u8),
}

impl From<u8> for RtrKind {
    fn from(dlc: u8) -> Self {
        match dlc {
            0x0 => RtrKind::AddressAcquisition,
            0x1 => RtrKind::AddressAnnounceAccept,
            0x2 => RtrKind::AddressAnnounceReject,
            0x5 => RtrKind::DiscoveryAccept,
            0x6 => RtrKind::DiscoveryReject,
            0x7 => RtrKind::DiscoveryAll,
            v => RtrKind::Other(v),
        }
    }
}

impl From<RtrKind> for u8 {
    fn from(k: RtrKind) -> u8 {
        match k {
            RtrKind::AddressAcquisition => 0x0,
            RtrKind::AddressAnnounceAccept => 0x1,
            RtrKind::AddressAnnounceReject => 0x2,
            RtrKind::DiscoveryAccept => 0x5,
            RtrKind::DiscoveryReject => 0x6,
            RtrKind::DiscoveryAll => 0x7,
            RtrKind::Other(v) => v,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShvCanId {
    // First data frame or priority bit for RTR frames
    pub first_prio: bool,
    pub device_addr: u8,
}

impl ShvCanId {
    pub fn to_raw_id(self) -> u16 {
        let mut id: u16 = 0;
        id |= 1 << 10; // SHVCAN
        id |= 1 << 9;  // Unused
        if self.first_prio  { id |= 1 << 8; }
        id |= (self.device_addr as u16) & 0xFF;
        id & 0x7FF
    }

    pub fn from_raw_id(raw: u16) -> Result<Self, ShvCanParseError> {
        if raw > 0x7FF {
            return Err(ShvCanParseError::InvalidCanId(raw as u32));
        }
        if (raw & (1 << 10)) != 0 || (raw & (1 << 9)) != 0 {
            // return Err(ShvParseError::InvalidCanId(raw as u32));
            return Err(ShvCanParseError::Malformed("Not a SHV CAN frame".into()));
        }
        let first = (raw & (1 << 8)) != 0;
        let device_addr = (raw & 0xFF) as u8;
        Ok(ShvCanId { first_prio: first, device_addr })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShvCanFrame {
    Data(DataFrame),
    Ack(AckFrame),
    Terminate(TerminateFrame),
    Remote(RemoteFrame),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFrameHeader {
    src: u8,
    dst: u8,
    first: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFrame {
    header: DataFrameHeader,
    counter: u8,
    payload: Vec<u8>,
}

impl DataFrame {
    pub fn new(src: u8, dst: u8, counter: u8, first: bool, data: &[u8]) -> Self {
        Self {
            header: DataFrameHeader { src, dst, first },
            counter,
            payload: data.into(),
        }
    }
}

impl TryFrom<DataFrame> for CanFdFrame {
    type Error = ShvCanParseError;

    fn try_from(frame: DataFrame) -> Result<Self, Self::Error> {
        let id = ShvCanId {
            first_prio: frame.header.first,
            device_addr: frame.header.src
        }.to_raw_id();
        let can_id = CanId::standard(id)
            .ok_or(ShvCanParseError::InvalidCanId(id as u32))?;
        let data = [&[frame.header.dst, frame.counter], frame.payload.as_slice()].concat();
        CanFdFrame::with_flags(can_id, &data, FdFlags::BRS | FdFlags::FDF)
            .ok_or_else(|| ShvCanParseError::FrameCreation("Cannot build a Data frame".into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckFrame {
    header: DataFrameHeader,
    counter: u8,
}

impl AckFrame {
    pub fn new(src: u8, dst: u8, counter: u8) -> Self {
        Self {
            header: DataFrameHeader { src, dst, first: false },
            counter,
        }
    }
}

impl TryFrom<AckFrame> for CanFdFrame {
    type Error = ShvCanParseError;

    fn try_from(frame: AckFrame) -> Result<Self, Self::Error> {
        let id = ShvCanId {
            first_prio: false,
            device_addr: frame.header.src
        }.to_raw_id();
        let can_id = CanId::standard(id)
            .ok_or(ShvCanParseError::InvalidCanId(id as u32))?;
        let data = &[frame.header.dst, frame.counter];
        CanFdFrame::with_flags(can_id, data, FdFlags::BRS | FdFlags::FDF)
            .ok_or_else(|| ShvCanParseError::FrameCreation("Cannot build an ACK frame".into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminateFrame {
    header: DataFrameHeader,
}

impl TerminateFrame {
    pub fn new(src: u8, dst: u8) -> Self {
        Self {
            header: DataFrameHeader { src, dst, first: true },
        }
    }
}

impl TryFrom<TerminateFrame> for CanFdFrame {
    type Error = ShvCanParseError;

    fn try_from(frame: TerminateFrame) -> Result<Self, Self::Error> {
        let id = ShvCanId {
            first_prio: true,
            device_addr: frame.header.src
        }.to_raw_id();
        let can_id = CanId::standard(id)
            .ok_or(ShvCanParseError::InvalidCanId(id as u32))?;
        let data = &[frame.header.dst];
        CanFdFrame::with_flags(can_id, data, FdFlags::BRS | FdFlags::FDF)
            .ok_or_else(|| ShvCanParseError::FrameCreation("Cannot build a Terminate frame".into()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFrame {
    src: u8,
    kind: RtrKind,
}

impl TryFrom<RemoteFrame> for CanRemoteFrame {
    type Error = ShvCanParseError;

    fn try_from(frame: RemoteFrame) -> Result<Self, Self::Error> {
        let priority = matches!(frame.kind, RtrKind::AddressAcquisition);
        let id = ShvCanId {
            first_prio: priority,
            device_addr: frame.src
        }.to_raw_id();
        let can_id = CanId::standard(id)
            .ok_or(ShvCanParseError::InvalidCanId(id as u32))?;
        CanRemoteFrame::new_remote(can_id, u8::from(frame.kind) as usize)
            .ok_or_else(|| ShvCanParseError::FrameCreation("Cannot build an RTR frame".into()))
    }
}

#[derive(Error, Debug)]
pub enum ShvCanParseError {
    #[error("Invalid CAN id: {0:#x}")]
    InvalidCanId(u32),
    #[error("Data frame too short (needs at least destination+counter)")]
    DataTooShort,
    #[error("Frame creation error: {0}")]
    FrameCreation(String),
    #[error("Malformed frame: {0}")]
    Malformed(String),
}

impl TryFrom<&CanFdFrame> for ShvCanFrame {
    type Error = ShvCanParseError;

    fn try_from(frame: &CanFdFrame) -> Result<Self, Self::Error> {
        let shv_can_id = ShvCanId::from_raw_id(frame.raw_id() as u16)?;
        let src = shv_can_id.device_addr;
        let data = frame.data();

        if data.is_empty() {
            return Err(ShvCanParseError::DataTooShort);
        }

        let dst = data[0];
        let header = DataFrameHeader {
            src,
            dst,
            first: shv_can_id.first_prio
        };

        match data.len() {
            1 => Ok(ShvCanFrame::Terminate(TerminateFrame { header })),
            2 => Ok(ShvCanFrame::Ack(AckFrame { header, counter: data[1] })),
            _ => {
                let counter = data[1];
                let mut payload = data[2..].to_vec();
                // Trim zero bytes of frames with DLC > 8
                if payload.len() > 8 {
                    trim_trailing_zeros(&mut payload);
                }
                Ok(ShvCanFrame::Data(DataFrame { header, counter, payload }))
            }
        }
    }
}

impl TryFrom<&CanRemoteFrame> for RemoteFrame {
    type Error = ShvCanParseError;

    fn try_from(frame: &CanRemoteFrame) -> Result<Self, Self::Error> {
        let shv_can_id = ShvCanId::from_raw_id(frame.raw_id() as u16)?;
        let src = shv_can_id.device_addr;
        let kind = RtrKind::from(frame.dlc() as u8);
        Ok(RemoteFrame { src, kind })
    }
}

pub struct CanFrameReader<R, W> {
    peer_id: PeerId,
    peer_addr: u8,
    frame_reader: R,
    ack_writer: W,
    last_start_frame_counter: Option<u8>,
    frame_size_limit: usize,
}

impl<R, W> CanFrameReader<R, W>
where
    R: StreamExt<Item = ShvCanFrame> + Unpin + Send,
    W: SinkExt<AckFrame> + Unpin + Send,
{
    pub fn new(frame_reader: R, ack_writer: W, peer_id: PeerId, peer_addr: u8) -> Self {
        Self {
            peer_id,
            peer_addr,
            frame_reader,
            ack_writer,
            last_start_frame_counter: None,
            frame_size_limit: DEFAULT_FRAME_SIZE_LIMIT,
        }
    }

    pub fn with_frame_size_limit(mut self, limit: usize) -> Self {
        self.frame_size_limit = limit;
        self
    }
}

fn trim_trailing_zeros(v: &mut Vec<u8>) {
    if let Some(pos) = v.iter().rposition(|&b| b != 0) {
        v.truncate(pos + 1);
    } else {
        v.clear();
    }
}

#[async_trait]
impl<R, W> FrameReader for CanFrameReader<R, W>
where
    R: Stream<Item = ShvCanFrame> + Unpin + Send,
    W: Sink<AckFrame> + Unpin + Send,
    <W as futures::Sink<AckFrame>>::Error: std::fmt::Display,
{
    fn peer_id(&self) -> PeerId {
       self.peer_id
    }

    fn frame_size_limit(&self) -> usize {
        self.frame_size_limit
    }

    async fn get_frame_bytes(&mut self) -> Result<Vec<u8>, ReceiveFrameError> {
        'start: loop {
            let mut frame = loop {
                let frame = self.frame_reader
                    .next()
                    .await
                    .ok_or_else(|| ReceiveFrameError::StreamError("Session terminated".into()))?;

                if let ShvCanFrame::Data(data_frame) = frame && data_frame.header.first {
                    break data_frame;
                };
            };

            'send_ack: loop {
                self.ack_writer
                    .send(AckFrame::new(frame.header.dst, frame.header.src, frame.counter))
                    .await
                    .map_err(|e| ReceiveFrameError::StreamError(format!("Session terminated while sending ACK: {e}")))?;

                let start_frame_counter = frame.counter & 0x7F;
                let is_last_frame = |frame: &DataFrame| frame.counter & 0x80 != 0;
                let mut res = Vec::new();
                loop {
                    res.append(&mut frame.payload);

                    if res.len() > self.frame_size_limit() {
                        return Err(ReceiveFrameError::FramingError(
                                format!("Client ID: {client_id}, address: {client_address}, Jumbo frames are not supported. Jumbo frame threshold is {frame_size_limit} bytes.",
                                    client_id = self.peer_id,
                                    client_address = self.peer_addr,
                                    frame_size_limit = self.frame_size_limit()
                                )
                        ))
                    }

                    if is_last_frame(&frame) {
                        if self.last_start_frame_counter.is_some_and(|last_start_frame_counter| last_start_frame_counter == start_frame_counter) {
                            // Receiving the same message multiple times in row - ignore it
                            continue 'start;
                        }
                        self.last_start_frame_counter = Some(start_frame_counter);
                        return Ok(res);
                    }

                    let next_frame_counter = frame.counter.saturating_add(1) & 0x7f;

                    frame = loop {
                        let frame = self.frame_reader
                            .next()
                            .await
                            .ok_or_else(|| ReceiveFrameError::StreamError("Session terminated".into()))?;

                        if let ShvCanFrame::Data(data_frame) = frame {
                            break data_frame;
                        };
                    };

                    // If the frame is a first frame, start over from sending the ACK, dropping the data fetched so far.
                    if frame.header.first {
                        continue 'send_ack;
                    }

                    // Start over with a new message on frame counter sequence violation
                    if frame.counter & 0x7f != next_frame_counter {
                        continue 'start;
                    }

                }
            }
        }
    }
}

const MAX_SEND_RETRIES_DEFAULT: u8 = 3;
const DELAY_BETWEEN_RETRIES_DEFAULT_MS: u64 = 100;

pub struct CanFrameWriter<W, R> {
    peer_id: PeerId,
    peer_addr: u8,
    device_addr: u8,
    frame_writer: W,
    ack_reader: R,
    start_frame_counter: u8,
    max_send_retries: u8,
    delay_between_retries: Duration,
}

impl<R, W> CanFrameWriter<W, R>
where
    W: SinkExt<ShvCanFrame> + Unpin + Send,
    R: StreamExt<Item = AckFrame> + Unpin + Send,
{
    pub fn new(frame_writer: W, ack_reader: R, peer_id: PeerId, peer_addr: u8, device_addr: u8) -> Self {
        Self {
            peer_id,
            peer_addr,
            device_addr,
            frame_writer,
            ack_reader,
            start_frame_counter: 0,
            max_send_retries: MAX_SEND_RETRIES_DEFAULT,
            delay_between_retries: Duration::from_millis(DELAY_BETWEEN_RETRIES_DEFAULT_MS),
        }
    }

    pub fn with_max_send_retries(mut self, retries_count: u8) -> Self {
        self.max_send_retries = retries_count;
        self
    }

    pub fn with_delay_between_retries(mut self, delay: Duration) -> Self {
        self.delay_between_retries = delay;
        self
    }
}

#[async_trait]
impl<W, R> FrameWriter for CanFrameWriter<W, R>
where
    W: SinkExt<ShvCanFrame> + Unpin + Send,
    R: StreamExt<Item = AckFrame> + Unpin + Send,
{
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    async fn send_frame_impl(&mut self, frame: RpcFrame) -> crate::Result<()> {
        let protocol = frame.protocol as u8;
        let meta = serialize_meta(&frame)?;
        let data = frame.data();

        let bytes_count = 1 + meta.len() + data.len();
        const MAX_PAYLOAD_SIZE: usize = 62;
        let frame_count = (bytes_count / MAX_PAYLOAD_SIZE) + 1;

        let start_frame_counter = self.start_frame_counter;
        self.start_frame_counter += 1;

        let to_frame_counter = |frame_idx: usize| {
            let val = start_frame_counter.saturating_add(frame_idx as u8) & 0x7f;
            if frame_idx == frame_count - 1 { val | 0x80 } else { val }
        };

        let mut bytes = [protocol].into_iter().chain(meta).chain(data.iter().copied());

        // Send the first frame and wait for the ACK
        let frame_payload = bytes.by_ref().take(MAX_PAYLOAD_SIZE).collect::<Vec<_>>();
        for retries_count in 0..=self.max_send_retries {
            let frame_counter = to_frame_counter(0);
            self
                .frame_writer
                .send(ShvCanFrame::Data(DataFrame::new(self.device_addr, self.peer_addr, frame_counter, true, &frame_payload)))
                .await
                .map_err(|_| "Session terminated")?;

            let ack_frame = self
                .ack_reader
                .next()
                .await
                .ok_or("Session terminated while waiting for ACK")?;

            if ack_frame.counter == frame_counter {
                break;
            } else if retries_count == self.max_send_retries {
                return Err(format!("Bad ACK, expected counter: {frame_counter}, received: {ack_counter}", ack_counter = ack_frame.counter).into());
            }
            futures_time::task::sleep(self.delay_between_retries).await;
        }

        for frame_idx in 1..frame_count {
            let frame_payload = bytes.by_ref().take(MAX_PAYLOAD_SIZE).collect::<Vec<_>>();
            self
                .frame_writer
                .send(ShvCanFrame::Data(DataFrame::new(self.device_addr, self.peer_addr, to_frame_counter(frame_idx), false, &frame_payload)))
                .await
                .map_err(|_| "Session terminated")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::future::join;
    use futures::{FutureExt, StreamExt};
    use socketcan::{CanFdFrame, CanId, EmbeddedFrame};

    use crate::framerw::{FrameReader, FrameWriter};
    use crate::canrw::CanFrameWriter;
    use crate::rpcframe::Protocol;
    use crate::canrw::{AckFrame, DataFrame, DataFrameHeader, ShvCanFrame, TerminateFrame};
    use crate::{RpcFrame, RpcMessage};

    use super::CanFrameReader;

    fn is_first_frame(can_id: u16) -> bool {
        can_id & (1 << 8) != 0
    }

    #[test]
    fn parse_frames() {
        {
            const CAN_ID: u16 = 0x124;
            let frame = CanFdFrame::new(CanId::standard(CAN_ID).unwrap(), &[42]).unwrap();
            let parsed = ShvCanFrame::try_from(&frame).unwrap();

            assert_eq!(
                parsed,
                ShvCanFrame::Terminate(TerminateFrame {
                    header: DataFrameHeader {
                        src: CAN_ID as u8,
                        dst: 42,
                        first: is_first_frame(CAN_ID),
                    }
                })
            );
        }
        {
            const CAN_ID: u16 = 0x125;
            let frame = CanFdFrame::new(CanId::standard(CAN_ID).unwrap(), &[7, 99]).unwrap();
            let parsed = ShvCanFrame::try_from(&frame).unwrap();

            assert_eq!(
                parsed,
                ShvCanFrame::Ack(AckFrame {
                    header: DataFrameHeader {
                        src: CAN_ID as u8,
                        dst: 7,
                        first: is_first_frame(CAN_ID),
                    },
                    counter: 99,
                })
            );
        }
        {
            const CAN_ID: u16 = 0x126;
            let frame = CanFdFrame::new(CanId::standard(CAN_ID).unwrap(), &[1, 2, 10, 20, 30]).unwrap();
            let parsed = ShvCanFrame::try_from(&frame).unwrap();

            assert_eq!(
                parsed,
                ShvCanFrame::Data(DataFrame {
                    header: DataFrameHeader {
                        src: CAN_ID as u8,
                        dst: 1,
                        first: is_first_frame(CAN_ID),
                    },
                    counter: 2,
                    payload: vec![10, 20, 30],
                })
            );
        }
        {
            const CAN_ID: u16 = 0x127;
            let data = &[1, 2, 10, 20, 30, 40, 50, 60, 70, 80];
            let frame = CanFdFrame::new(CanId::standard(CAN_ID).unwrap(), data).unwrap();
            let parsed = ShvCanFrame::try_from(&frame).unwrap();

            assert_eq!(
                parsed,
                ShvCanFrame::Data(DataFrame {
                    header: DataFrameHeader {
                        src: CAN_ID as u8,
                        dst: 1,
                        first: is_first_frame(CAN_ID),
                    },
                    counter: 2,
                    payload: data[2..].into(),
                })
            );
        }
    }

    #[async_std::test]
    async fn send_reset_session() {
        let (ack_tx, ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, mut frames_rx) = futures::channel::mpsc::unbounded();
        let mut wr = CanFrameWriter::new(frames_tx, ack_rx, 0, 0x23, 0x01);

        let receiver = pin!(async move {
            // Receive the reset session frame
            let ShvCanFrame::Data(data_frame) = frames_rx
                .next()
                .await
                .expect("Expected reset session frame") else {
                    panic!("Reset session is not a data frame");
            };
            assert_eq!(data_frame.header.src, 0x1);
            assert_eq!(data_frame.header.dst, 0x23);
            assert!(data_frame.header.first);
            assert_eq!(data_frame.payload, vec![Protocol::ResetSession as u8]);
            // Send ACK
            ack_tx.unbounded_send(AckFrame::new(data_frame.header.dst, data_frame.header.src, data_frame.counter)).unwrap();
        }.fuse());

        let (_, sender_res) = join(receiver, wr.send_reset_session()).await;
        assert!(sender_res.is_ok());
    }

    #[async_std::test]
    async fn resend_on_bad_ack() {
        let (ack_tx, ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, mut frames_rx) = futures::channel::mpsc::unbounded();
        let mut wr = CanFrameWriter::new(frames_tx, ack_rx, 0, 0x23, 0x01);

        let receiver = pin!(async move {
            for _ in 0..3 {
                // Receive the reset session frame
                let ShvCanFrame::Data(data_frame) = frames_rx
                    .next()
                    .await
                    .expect("Expected reset session frame") else {
                        panic!("Reset session is not a data frame");
                    };
                assert_eq!(data_frame.header.src, 0x1);
                assert_eq!(data_frame.header.dst, 0x23);
                assert!(data_frame.header.first);
                assert_eq!(data_frame.payload, vec![Protocol::ResetSession as u8]);
                // Send wrong ACK
                ack_tx.unbounded_send(AckFrame::new(data_frame.header.dst, data_frame.header.src, data_frame.counter.saturating_add(1))).unwrap();
            }
            // Receive the reset session frame
            let ShvCanFrame::Data(data_frame) = frames_rx
                .next()
                .await
                .expect("Expected reset session frame") else {
                    panic!("Reset session is not a data frame");
                };
            assert_eq!(data_frame.header.src, 0x1);
            assert_eq!(data_frame.header.dst, 0x23);
            assert!(data_frame.header.first);
            assert_eq!(data_frame.payload, vec![Protocol::ResetSession as u8]);
            // Send good ACK
            ack_tx.unbounded_send(AckFrame::new(data_frame.header.dst, data_frame.header.src, data_frame.counter)).unwrap();
        }.fuse());

        let (_, sender_res) = join(receiver, wr.send_reset_session()).await;
        assert!(sender_res.is_ok());
    }

    #[async_std::test]
    async fn fail_on_bad_ack() {
        let (ack_tx, ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, mut frames_rx) = futures::channel::mpsc::unbounded();
        let mut wr = CanFrameWriter::new(frames_tx, ack_rx, 0, 0x23, 0x01);

        let receiver = pin!(async move {
            for _ in 0..4 {
                // Receive the reset session frame
                let ShvCanFrame::Data(data_frame) = frames_rx
                    .next()
                    .await
                    .expect("Expected reset session frame") else {
                        panic!("Reset session is not a data frame");
                    };
                assert_eq!(data_frame.header.src, 0x1);
                assert_eq!(data_frame.header.dst, 0x23);
                assert!(data_frame.header.first);
                assert_eq!(data_frame.payload, vec![Protocol::ResetSession as u8]);
                // Send wrong ACK
                ack_tx.unbounded_send(AckFrame::new(data_frame.header.dst, data_frame.header.src, data_frame.counter.saturating_add(1))).unwrap();
            }
        }.fuse());

        let (_, sender_res) = join(receiver, wr.send_reset_session()).await;
        assert!(sender_res.is_err());
    }

    const CHAINPACK: u8 = Protocol::ChainPack as u8;

    async fn run_send_rpc_message_test(msg: RpcMessage, expected_payloads: &[&[u8]]) {
        let (ack_tx, ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, mut frames_rx) = futures::channel::mpsc::unbounded();

        const PEER_ADDR: u8 = 0x23;
        const DEVICE_ADDR: u8 = 0x01;

        let mut send_fut = pin!(async move {
            let mut wr = CanFrameWriter::new(frames_tx, ack_rx, 0, PEER_ADDR, DEVICE_ADDR);
            wr.send_message(msg).await
        }.fuse());

        let mut frame_count = 0;
        let mut start_counter = 0;

        loop {
            futures::select! {
                res = send_fut => {
                    res.unwrap_or_else(|e| panic!("Send message failed: {e}"));
                }

                frame = frames_rx.select_next_some() => {
                    match frame {
                        ShvCanFrame::Data(data_frame) => {
                            assert_eq!(data_frame.header.src, DEVICE_ADDR);
                            assert_eq!(data_frame.header.dst, PEER_ADDR);
                            assert_eq!(data_frame.header.first, frame_count == 0);
                            if data_frame.header.first {
                                start_counter = data_frame.counter & 0x7f;
                                ack_tx.unbounded_send(AckFrame::new(
                                        data_frame.header.dst,
                                        data_frame.header.src,
                                        data_frame.counter
                                )).unwrap();
                            } else {
                                assert_eq!(data_frame.counter, (start_counter.saturating_add(frame_count as u8) & 0x7f) | if frame_count == expected_payloads.len() - 1 { 0x80 } else { 0 });
                            }
                            assert_eq!(data_frame.payload, expected_payloads[frame_count].to_vec());

                            frame_count += 1;
                        }
                        _ => panic!("Not a data frame"),
                    }
                }
                complete => break,
            }
        }

        assert_eq!(frame_count, expected_payloads.len());
    }

    #[async_std::test]
    async fn send_rpc_message() {
        let msg = RpcMessage::create_request_with_id(1, "foo/bar", "xyz", Some(42.into()));

        let expected_payloads: &[&[u8]] = &[&[
            CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
            0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
            0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x6a, 0xff
        ]];

        run_send_rpc_message_test(msg, expected_payloads).await;
    }

    #[async_std::test]
    async fn send_rpc_message_multiframe() {
        let msg = RpcMessage::create_request_with_id(
            1, "foo/bar", "xyz",
            Some("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".into())
        );

        let expected_payloads: &[&[u8]] = &[
            &[
                CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                0x77, 0x78, 0x79
            ],
            &[
                0x7a, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
                0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53,
                0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0xff
            ]
        ];

        run_send_rpc_message_test(msg, expected_payloads).await;
    }

    async fn run_receive_rpc_frame_test(rpc_frame: RpcFrame, payloads: &[&[u8]]) {
        let (ack_tx, mut ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, frames_rx) = futures::channel::mpsc::unbounded();

        const PEER_ADDR: u8 = 0x23;
        const DEVICE_ADDR: u8 = 0x01;

        let mut rd = CanFrameReader::new(frames_rx, ack_tx, 0, PEER_ADDR);

        let mut send_frames = pin!(async move {
            for (frame_idx, payload) in payloads.iter().copied().enumerate() {
                let counter = frame_idx as u8 & 0x7f | if frame_idx == payloads.len() - 1 { 0x80 } else { 0 };
                let frame = ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        counter,
                        frame_idx == 0,
                        payload)
                );
                frames_tx.unbounded_send(frame).unwrap();

                if frame_idx == 0 {
                    let ack_frame = ack_rx.next().await.expect("Receiver should send ACK");
                    assert!(!ack_frame.header.first);
                    assert_eq!(ack_frame.header.src, DEVICE_ADDR);
                    assert_eq!(ack_frame.header.dst, PEER_ADDR);
                    assert_eq!(ack_frame.counter, counter);
                }
            }
        }.fuse());

        let mut read_fut = rd.receive_frame().fuse();

        loop {
            futures::select! {
                _ = send_frames => { }
                res = read_fut => {
                    let received_rpc_frame = res.expect("Valid RpcFrame");
                    assert_eq!(rpc_frame, received_rpc_frame);
                }
                complete => break,
            }
        }
    }

    #[async_std::test]
    async fn receive_frame() {
        let rpc_frame = RpcMessage::create_request_with_id(1, "foo/bar", "xyz", Some(42.into())).to_frame().unwrap();

        let payloads: &[&[u8]] = &[&[
            CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
            0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
            0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x6a, 0xff
        ]];

        run_receive_rpc_frame_test(rpc_frame, payloads).await;
    }

    #[async_std::test]
    async fn receive_rpc_frame_more_payloads() {
        let frame = RpcMessage::create_request_with_id(
            1, "foo/bar", "xyz",
            Some("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".into())
        ).to_frame().unwrap();

        let payloads: &[&[u8]] = &[
            &[
                CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                0x77, 0x78, 0x79
            ],
            &[
                0x7a, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
                0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53,
                0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0xff
            ]
        ];

        run_receive_rpc_frame_test(frame, payloads).await;
    }

    #[async_std::test]
    async fn counter_changes_for_new_message() {
        let (ack_tx, ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, mut frames_rx) = futures::channel::mpsc::unbounded();

        const PEER_ADDR: u8 = 0x23;
        const DEVICE_ADDR: u8 = 0x01;

        let msgs = [
            RpcMessage::create_request_with_id(1, "foo/bar", "xyz", Some(42.into())),
            RpcMessage::create_request_with_id(2, "foo/bar", "baz", Some(true.into())),
            RpcMessage::create_request_with_id(3, "foo/bar", "anything", Some("abcd".into())),
        ];

        let mut send_fut = pin!(async move {
            let mut wr = CanFrameWriter::new(frames_tx, ack_rx, 0, PEER_ADDR, DEVICE_ADDR);
            for msg in msgs {
                wr.send_message(msg).await.unwrap();
            }
        }.fuse());

        let mut start_counter = None;

        loop {
            futures::select! {
                _ = send_fut => { }

                frame = frames_rx.select_next_some() => {
                    match frame {
                        ShvCanFrame::Data(data_frame) => {
                            assert_eq!(data_frame.header.src, DEVICE_ADDR);
                            assert_eq!(data_frame.header.dst, PEER_ADDR);
                            if data_frame.header.first {
                                if let Some(counter) = start_counter {
                                    assert_ne!(counter, data_frame.counter, "A counter value should be different for a new message");
                                }
                                start_counter = Some(data_frame.counter);
                                ack_tx.unbounded_send(AckFrame::new(
                                        data_frame.header.dst,
                                        data_frame.header.src,
                                        data_frame.counter
                                )).unwrap();
                            }
                        }
                        _ => panic!("Expected a data frame"),
                    }
                }
                complete => break,
            }
        }
    }

    #[async_std::test]
    async fn read_and_write() {
        let (ack_tx, ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, frames_rx) = futures::channel::mpsc::unbounded();

        const PEER_ADDR: u8 = 0x23;
        const DEVICE_ADDR: u8 = 0x01;

        let frames = [
            RpcMessage::create_request_with_id(1, "foo/bar", "xyz", Some(42.into())).to_frame().unwrap(),
            RpcMessage::create_request_with_id(2, "foo/bar", "baz", Some(true.into())).to_frame().unwrap(),
            RpcMessage::create_request_with_id(3, "foo/bar", "anything", Some("abcd".into())).to_frame().unwrap(),
        ];

        let mut wr = CanFrameWriter::new(frames_tx, ack_rx, 0, PEER_ADDR, DEVICE_ADDR);
        let mut rd = CanFrameReader::new(frames_rx, ack_tx, 0, PEER_ADDR);

        for frame in frames {
            let (wr_res, rd_res) = join(wr.send_frame(frame.clone()), rd.receive_frame()).await;
            assert!(wr_res.is_ok());
            assert_eq!(frame, rd_res.unwrap());
        }

    }

    #[async_std::test]
    async fn read_skips_frame_counter_violation() {
        let (ack_tx, mut ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, frames_rx) = futures::channel::mpsc::unbounded();

        const PEER_ADDR: u8 = 0x23;
        const DEVICE_ADDR: u8 = 0x01;

        let mut rd = CanFrameReader::new(frames_rx, ack_tx, 0, PEER_ADDR);

        let send_frames = pin!(async move {
            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        0,
                        true,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                        0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                        0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                        0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                        0x77, 0x78, 0x79
                        ],
            ))).unwrap();

            let ack_frame = ack_rx.next().await.expect("Receiver should send ACK");
            assert!(!ack_frame.header.first);
            assert_eq!(ack_frame.header.src, DEVICE_ADDR);
            assert_eq!(ack_frame.header.dst, PEER_ADDR);
            assert_eq!(ack_frame.counter, 0);

            // Invalid frames
            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        5,
                        false,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                        0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                        0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                        0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                        0x77, 0x78, 0x79
                        ],
            ))).unwrap();

            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        4,
                        false,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                        0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                        0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                        0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                        0x77, 0x78, 0x79
                        ],
            ))).unwrap();

            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        6,
                        false,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                        0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                        0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                        0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                        0x77, 0x78, 0x79
                        ],
            ))).unwrap();

            // Restore the sequence with a new start frame
            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        2,
                        true,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x86, 0x3e, 0x30, 0x31,
                        0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62,
                        0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
                        0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76,
                        0x77, 0x78, 0x79
                        ],
            ))).unwrap();

            let ack_frame = ack_rx.next().await.expect("Receiver should send ACK");
            assert!(!ack_frame.header.first);
            assert_eq!(ack_frame.header.src, DEVICE_ADDR);
            assert_eq!(ack_frame.header.dst, PEER_ADDR);
            assert_eq!(ack_frame.counter, 2);

            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        3 | 0x80,
                        false,
                        &[
                        0x7a, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
                        0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53,
                        0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0xff
                        ]
            ))).unwrap();

        }.fuse());

        let read_fut = pin!(async move {
            let frame = rd.receive_frame().await.unwrap();
            assert_eq!(
                RpcMessage::create_request_with_id(
                    1,
                    "foo/bar",
                    "xyz",
                    Some("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".into())
                ).to_frame().unwrap(),
                frame
            );
        }.fuse());

        join(send_frames, read_fut).await;
    }

    #[async_std::test]
    async fn read_skips_duplicate_frames() {
        let (ack_tx, mut ack_rx) = futures::channel::mpsc::unbounded();
        let (frames_tx, frames_rx) = futures::channel::mpsc::unbounded();

        const PEER_ADDR: u8 = 0x23;
        const DEVICE_ADDR: u8 = 0x01;

        let mut rd = CanFrameReader::new(frames_rx, ack_tx, 0, PEER_ADDR);

        let read_fut = pin!(async move {
            let frame = rd.receive_frame().await.unwrap();
            assert_eq!(frame, RpcMessage::create_request_with_id(1, "foo/bar", "xyz", Some(42.into())).to_frame().unwrap());
            let frame = rd.receive_frame().await.unwrap();
            assert_eq!(frame, RpcMessage::create_request_with_id(2, "foo/bar", "xyz", Some(43.into())).to_frame().unwrap());
        }.fuse());

        let send_frames = pin!(async move {
            let counter = 42 | 0x80;

            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        counter,
                        true,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x6a, 0xff
                        ],
            ))).unwrap();

            let ack_frame = ack_rx.next().await.expect("Receiver should send ACK");
            assert!(!ack_frame.header.first);
            assert_eq!(ack_frame.header.src, DEVICE_ADDR);
            assert_eq!(ack_frame.header.dst, PEER_ADDR);
            assert_eq!(ack_frame.counter, counter);

            // Duplicate frames
            for _ in 0..5 {
                frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                            PEER_ADDR,
                            DEVICE_ADDR,
                            counter,
                            true,
                            &[
                            CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x41, 0x49, 0x86, 0x07,
                            0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                            0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x6a, 0xff
                            ],
                ))).unwrap();

                let ack_frame = ack_rx.next().await.expect("Receiver should send ACK");
                assert!(!ack_frame.header.first);
                assert_eq!(ack_frame.header.src, DEVICE_ADDR);
                assert_eq!(ack_frame.header.dst, PEER_ADDR);
                assert_eq!(ack_frame.counter, counter);
            }

            // Send a new frame
            let counter = 24 | 0x80;
            frames_tx.unbounded_send(ShvCanFrame::Data(DataFrame::new(
                        PEER_ADDR,
                        DEVICE_ADDR,
                        counter,
                        true,
                        &[
                        CHAINPACK, 0x8b, 0x41, 0x41, 0x48, 0x42, 0x49, 0x86, 0x07,
                        0x66, 0x6f, 0x6f, 0x2f, 0x62, 0x61, 0x72, 0x4a, 0x86, 0x03,
                        0x78, 0x79, 0x7a, 0xff, 0x8a, 0x41, 0x6b, 0xff
                        ],
            ))).unwrap();

            let ack_frame = ack_rx.next().await.expect("Receiver should send ACK");
            assert!(!ack_frame.header.first);
            assert_eq!(ack_frame.header.src, DEVICE_ADDR);
            assert_eq!(ack_frame.header.dst, PEER_ADDR);
            assert_eq!(ack_frame.counter, counter);
        }.fuse());

        join(send_frames, read_fut).await;
    }
}
