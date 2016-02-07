extern crate lmdb;
extern crate uuid;
extern crate capnp;
extern crate byteorder;

mod unbase_capnp {
    include!(concat!(env!("OUT_DIR"), "/unbase_capnp.rs"));
}
use lmdb::{RoTransaction, RwTransaction, Transaction, WriteFlags};
use std::collections::HashSet;
use uuid::Uuid;
use byteorder::{ByteOrder, NativeEndian};

mod keys {
    pub const SIGNATURE : u8 = 1;
    pub const SIGVERSION : u8 = 2;
    pub const SYSTEM_UUID : u8 = 3;
    pub const DATABASE_UUID : u8 = 4;
    pub const SUBSCRIBED_PREFIXES : u8 = 5;
    pub const STATE_BY_ID : u8 = 6;
    pub const UP_SYSTEM_UUID : u8 = 7;
    pub const UP_SYSTEM_EPOCH : u8 = 8;
    pub const THIS_SYSTEM_EPOCH : u8 = 9;
    pub const KNOWN_CLOCKS : u8 = 10;
    pub const SUBSCRIBED_NAMES : u8 = 11;

    pub const SIGNATURE_VALUE : &'static str = "Unbase/T database";
    pub const SIGVERSION_VALUE : u32 = 0x10000;
}

macro_rules! push_list {
    ($vec:expr, ...$list:expr, $($rest:tt)*) => {
        $vec.extend($list);
        push_list!($vec, $($rest)*);
    };
    ($vec:expr, ...$list:expr) => { push_list!($vec, ...$list,) };
    ($vec:expr, $item:expr, $($rest:tt)*) => {
        $vec.push($item);
        push_list!($vec, $($rest)*);
    };
    ($vec:expr, $item:expr) => { push_list!($vec, $item,) };
    ($vec:expr,) => {};
}

macro_rules! vec2 {
    ($($part:tt)*) => {{
        let mut vec = Vec::new();
        push_list!(vec, $($part)*);
        vec
    }};
}

// for the second pass, our data is stored in an LMDB object
// LMDB keys:
// #1 : general bookkeeping data
// #1 #1 downstream-id #1 counter : pending messages
// something about subtree subscriptions?
// #1 #2 : my uuid
// #1 #3 : database uuid
// #2 : data per object that is tracked in the system
// #2 object-id : upstream data (must exist, we are an inclusive cache), our version if different
// individual subscriptions go here?
// #3 : data per index (upstream or downstream?? not sure)

// Commands -\
// UpState --+-> OurState -\
//        \----------------+-> StateDiff-\
// UpIndex ------------------------------+-> OurIndex

// we should *not* tightly couple CRDT flags to the audit trail

// TODO(suggestion): add as_ro() to RwCursor and RwTransaction ?
// TODO(suggestion): Iter/IterDup needs docs
// TODO(suggestion): IterRange?
// TODO(suggestion): IterDup api is kinda unsafe?

struct UnbaseEngine {
    env: lmdb::Environment,
}

struct ObjectId(Vec<u8>);

enum ObjectState {
    Exists(Vec<u8>),
    NotExists,
    NoData,
}

struct Error;
impl From<lmdb::Error> for Error {
    fn from(e: lmdb::Error) -> Error {
        unimplemented!()
    }
}
type Result<T> = std::result::Result<T, Error>;

// TODO(soon): actually use read contexts
struct ReadContext<'txn> {
    tx: &'txn lmdb::RoTransaction<'txn>,
    db: lmdb::Database,
}

struct WriteContext<'txn> {
    tx: &'txn mut lmdb::RwTransaction<'txn>,
    db: lmdb::Database,
    current_epoch: i64,
}

impl<'txn> WriteContext<'txn> {
    fn as_ro(&self) -> ReadContext<'txn> {
        unimplemented!()
    }
}

// TODO upstream
fn get_opt<'txn, TX : Transaction, K : AsRef<[u8]>>(txn: &'txn TX, database: lmdb::Database, key: &K) -> lmdb::Result<Option<&'txn [u8]>> {
    match txn.get(database, key) {
        Ok(val) => Ok(Some(val)),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

fn probe_prefixes(ctx: &WriteContext, id: &[u8]) -> Result<bool> {
    let mut prefix_len = id.len();
    loop {
        let key = vec2![keys::SUBSCRIBED_PREFIXES, ...&id[0 .. prefix_len]];
        if try!(get_opt(ctx.tx, ctx.db, &key)).is_some() { return Ok(true); }
        if prefix_len == 0 { return Ok(false); }

        prefix_len -= 1;
        while prefix_len > 0 && id[prefix_len - 1] >= 128 { prefix_len -= 1; }
    }
}

fn has_up_system(ctx: &WriteContext) -> Result<bool> {
    let val = try!(get_opt(ctx.tx, ctx.db, &[keys::UP_SYSTEM_UUID]));
    Ok(val.is_some())
}

fn probe_name(ctx: &WriteContext, id: &[u8]) -> Result<bool> {
    if !try!(has_up_system(ctx)) {
        return Ok(true); // root system has all
    }

    let key = vec2![keys::SUBSCRIBED_NAMES, ...id];
    if try!(get_opt(ctx.tx, ctx.db, &key)).is_some() { return Ok(true); }
    if try!(probe_prefixes(ctx, id)) { return Ok(true); }
    Ok(false)
}

fn bytes_to_u32(bytes: &[u8]) -> Option<u32> {
    if bytes.len() == 4 { Some(NativeEndian::read_u32(bytes)) } else { None }
}

fn bytes_to_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 8 { Some(NativeEndian::read_i64(bytes)) } else { None }
}

fn i64_to_bytes(data: i64) -> [u8; 8] {
    let mut buf = [0; 8];
    NativeEndian::write_i64(&mut buf, data);
    buf
}

fn increment_epoch(ctx: &mut WriteContext) -> Result<()> {
    ctx.current_epoch += 1; // TODO(soon) overflow
    try!(ctx.tx.put(ctx.db, &vec![keys::THIS_SYSTEM_EPOCH], &i64_to_bytes(ctx.current_epoch), WriteFlags::empty()));
    Ok(())
}

enum UpdateDataResult {
    NotReady,
}

fn update_data(ctx: &WriteContext, entity_id: &[u8], operation_id: Option<&[u8]>, operation_data: &[u8]) -> Result<UpdateDataResult> {
    if !try!(probe_name(ctx, entity_id)) { return Ok(UpdateDataResult::NotReady); }
    match operation_id {
        Some(op_id) => {
            if !try!(probe_name(ctx, op_id)) { return Ok(UpdateDataResult::NotReady); }
            // TODO check for non-initial state
        }
        None => {
            // TODO check if current state dominates operation_data
        }
    }
    unimplemented!()
    // TODO append to local operation buffer
    // TODO apply to local state
    // TODO apply changes in local state to local delta indices
}

fn update_clock(ctx: &mut WriteContext, system: &Uuid, clock: i64) -> Result<()> {
    let clock_key = vec2![keys::KNOWN_CLOCKS, ...system.as_bytes()];
    let old_clock = try!(get_opt(ctx.tx, ctx.db, &clock_key)).and_then(bytes_to_i64).unwrap_or(0);
    if clock > old_clock {
        try!(ctx.tx.put(ctx.db, &clock_key, &i64_to_bytes(clock), WriteFlags::empty()));
        // TODO record and forward change
    }
    Ok(())
}

fn apply_replicate_down() {
    // bump UP_SYSTEM_EPOCH
    // clear out local operations which are included
    //     start recording potential changes to our computed state
    // apply replicated state changes
    //     compute changes to computed state
    // apply computed state changes to delta-index
    // apply changes to upstream-index
    // update clocks passed down
}

fn apply_replicate_up() {
    // bump our included-value
    // update_data for each passed up operation
}

fn apply_replicate_sibling() {
    // update passed clocks
    // update_data for each passed operation
}

fn epoch_bump_index(tx: &mut lmdb::RwTransaction) {
}

#[test]
fn it_works() {
}
