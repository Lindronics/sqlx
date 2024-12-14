use std::io::Read;

use sqlx_core::{
    bytes::{Buf, Bytes},
    io::ProtocolDecode,
};

#[derive(Debug)]
pub enum Replication {
    XLogData(XLogData),
    PrimaryKeepalive(PrimaryKeepalive),
    // StandbyStatusUpdate,
    // HotStandbyFeedback,
}

impl ProtocolDecode<'_> for Replication {
    fn decode_with(mut buf: Bytes, context: ()) -> Result<Self, sqlx_core::Error> {
        let format = buf.get_u8();

        match format {
            b'w' => XLogData::decode_with(buf, context).map(Self::XLogData),
            b'k' => PrimaryKeepalive::decode_with(buf, context).map(Self::PrimaryKeepalive),
            x => Err(err_protocol!(
                "unknown replication message type: {:?}",
                x as char
            )),
        }
    }
}

#[derive(Debug)]
pub struct XLogData {
    pub wal_start: i64,
    pub wal_end: i64,
    pub timestamp: i64,
    pub data: Bytes,
}

impl ProtocolDecode<'_> for XLogData {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            wal_start: buf.get_i64(),
            wal_end: buf.get_i64(),
            timestamp: buf.get_i64(),
            data: buf,
        })
    }
}

#[derive(Debug)]
pub struct PrimaryKeepalive {
    pub wal_end: i64,
    pub timestamp: i64,
    pub reply: u8,
}

impl ProtocolDecode<'_> for PrimaryKeepalive {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            wal_end: buf.get_i64(),
            timestamp: buf.get_i64(),
            reply: buf.get_u8(),
        })
    }
}

#[derive(Debug)]
pub enum LogicalReplication {
    Begin,
    Message,
    Commit(Commit),
    Origin,
    Relation,
    Type,
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate,
    StreamStart,
    StreamStop,
    StreamCommit,
    StreamAbort,

    // Since version 3
    BeginPrepare,
    Prepare,
    CommitPrepared,
    RollbackPrepared,
    StreamPrepare,
}

impl ProtocolDecode<'_> for LogicalReplication {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        let format = buf.get_u8();
        match format {
            b'B' => Ok(Self::Begin),
            b'M' => Ok(Self::Message),
            b'C' => Ok(Self::Commit(Commit::decode(buf)?)),
            b'O' => Ok(Self::Origin),
            b'R' => Ok(Self::Relation),
            b'Y' => Ok(Self::Type),
            b'I' => Ok(Self::Insert(Insert::decode(buf)?)),
            b'U' => Ok(Self::Update(Update::decode(buf)?)),
            b'D' => Ok(Self::Delete(Delete::decode(buf)?)),
            b'T' => Ok(Self::Truncate),
            b'S' => Ok(Self::StreamStart),
            b'E' => Ok(Self::StreamStop),
            b'c' => Ok(Self::StreamCommit),
            b'A' => Ok(Self::StreamAbort),
            b'b' => Ok(Self::BeginPrepare),
            b'P' => Ok(Self::Prepare),
            b'K' => Ok(Self::CommitPrepared),
            b'r' => Ok(Self::RollbackPrepared),
            b'p' => Ok(Self::StreamPrepare),
            x => Err(err_protocol!(
                "unknown logical replication message type: {:?}",
                x as char
            )),
        }
    }
}

#[derive(Debug)]
pub struct Commit {
    pub commit_lsn: i64,
    pub flags: i8,
    pub transaction_lsn: i64,
    pub timestamp: i64,
}

impl ProtocolDecode<'_> for Commit {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            commit_lsn: buf.get_i64(),
            flags: buf.get_i8(),
            transaction_lsn: buf.get_i64(),
            timestamp: buf.get_i64(),
        })
    }
}

#[derive(Debug)]
pub struct Insert {
    pub transaction_id: i32,
    pub oid: i32,
    pub data: Tuples,
}

impl ProtocolDecode<'_> for Insert {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        let transaction_id = buf.get_i32();
        let oid = buf.get_i32();

        if buf.get_u8() != b'N' {
            return Err(err_protocol!("expected new data"));
        }
        let data = Tuples::decode(&mut buf)?;

        Ok(Self {
            transaction_id,
            oid,
            data,
        })
    }
}

#[derive(Debug)]
pub struct Update {
    pub transaction_id: i32,
    pub oid: i32,
    pub old_data: Option<Tuples>,
    pub key_data: Option<Tuples>,
    pub new_data: Tuples,
}

impl ProtocolDecode<'_> for Update {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        let transaction_id = buf.get_i32();
        let oid = buf.get_i32();
        let (key_data, old_data) = match buf.first() {
            Some(b'K') => {
                buf.advance(1);
                (Some(Tuples::decode(&mut buf)?), None)
            }
            Some(b'O') => {
                buf.advance(1);
                (None, Some(Tuples::decode(&mut buf)?))
            }
            _ => (None, None),
        };

        if buf.get_u8() != b'N' {
            return Err(err_protocol!("expected new data"));
        }

        let new_data = Tuples::decode(&mut buf)?;

        Ok(Self {
            transaction_id,
            oid,
            key_data,
            old_data,
            new_data,
        })
    }
}

#[derive(Debug)]
pub struct Delete {
    pub transaction_id: i32,
    pub oid: i32,
    pub key_data: Option<Tuples>,
    pub old_data: Option<Tuples>,
}

impl ProtocolDecode<'_> for Delete {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, sqlx_core::Error> {
        let transaction_id = buf.get_i32();
        let oid = buf.get_i32();

        // TODO panics
        let (key_data, old_data) = match buf.get_u8() {
            b'K' => (Some(Tuples::decode(&mut buf)?), None),
            b'O' => (None, Some(Tuples::decode(&mut buf)?)),
            _ => (None, None),
        };

        Ok(Self {
            transaction_id,
            oid,
            key_data,
            old_data,
        })
    }
}

#[derive(Debug)]
pub struct Tuples(pub Vec<TupleData>);

impl Tuples {
    fn decode(buf: &mut Bytes) -> Result<Self, sqlx_core::Error> {
        let n_cols = buf.get_i16();
        #[allow(clippy::cast_sign_loss)]
        let mut tuple_data = Vec::with_capacity(n_cols as usize);
        for _ in 0..n_cols {
            tuple_data.push(TupleData::decode(buf)?);
        }
        Ok(Self(tuple_data))
    }
}

#[derive(Debug)]
pub enum TupleData {
    Null,
    UnchangedToast,
    Text(Bytes),
    Binary(Bytes),
}

impl TupleData {
    fn decode(buf: &mut Bytes) -> Result<Self, sqlx_core::Error> {
        match buf.get_u8() {
            b'n' => Ok(Self::Null),
            b'u' => Ok(Self::UnchangedToast),
            b't' => {
                let len = buf.get_i32();
                #[allow(clippy::cast_sign_loss)]
                let mut data = vec![0; len as usize];
                buf.reader().read_exact(&mut data)?;
                Ok(TupleData::Text(Bytes::from(data)))
            }
            b'b' => {
                let len = buf.get_i32();
                #[allow(clippy::cast_sign_loss)]
                let mut data = vec![0; len as usize];
                buf.reader().read_exact(&mut data)?;
                Ok(TupleData::Binary(Bytes::from(data)))
            }
            x => Err(err_protocol!("unknown typle data type: {:?}", x as char)),
        }
    }
}
