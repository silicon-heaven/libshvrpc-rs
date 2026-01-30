pub enum Tag {
    MetaTypeId = 1,
    MetaTypeNameSpaceId,
    USER = 8
}

pub mod global_ns {
    pub const NAMESPACE_ID: i64 = 0;

    pub enum MetaTypeID
    {
        ChainPackRpcMessage = 1,
        RpcConnectionParams,
        TunnelCtl,
        AccessGrantLogin,
        ValueChange,
        NodeDrop,
        ShvJournalEntry = 8,
        NodePropertyMap,
    }
}
