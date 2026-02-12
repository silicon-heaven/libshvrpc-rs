pub mod framerw;
pub mod client;
pub mod metamethod;
pub mod rpc;
pub mod rpctype;
pub mod rpcframe;
pub mod rpcmessage;
pub mod rpcdiscovery;
pub mod serialrw;
#[cfg(feature = "can")]
pub mod canrw;
pub mod streamrw;
#[cfg(feature = "websocket")]
pub mod websocketrw;
pub mod util;
#[cfg(feature = "journal")]
pub mod journalrw;
#[cfg(feature = "journal")]
pub mod journalentry;
#[cfg(feature = "journal")]
pub mod datachange;
pub mod typeinfo;

pub use rpcmessage::{RpcMessage, RpcMessageMetaTags};
pub use rpcframe::RpcFrame;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
