extern crate lmdb;
extern crate uuid;
extern crate capnp;

mod unbase_capnp {
    include!(concat!(env!("OUT_DIR"), "/unbase_capnp.rs"));
}
use lmdb::{RoTransaction, Transaction};
use std::collections::HashSet;
use uuid::Uuid;

mod keys {
    pub const SIGNATURE : u8 = 1;
    pub const SIGVERSION : u8 = 2;
    pub const SYSTEM_UUID : u8 = 3;
    pub const DATABASE_UUID : u8 = 4;
    pub const SUBSCRIBED_PREFIXES : u8 = 5;
    pub const STATE_BY_ID : u8 = 6;
    pub const UP_SYSTEM_UUID : u8 = 7;
    pub const UP_SYSTEM_EPOCH : u8 = 8;

    pub const SIGNATURE_VALUE : &'static str = "Unbase/T database";
    pub const SIGVERSION_VALUE : u32 = 0x10000;
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
}
type Result<T> = std::result::Result<T, Error>;

struct ReadContext<'txn> {
    tx: &'txn lmdb::RoTransaction<'txn>,
    db: lmdb::Database,
}

struct WriteContext<'txn> {
    tx: &'txn lmdb::RwTransaction<'txn>,
    db: lmdb::Database,
    updated_keys: HashSet<Vec<u8>>,
}

impl<'txn> WriteContext<'txn> {
    fn as_ro(&self) -> ReadContext<'txn> {
        unimplemented!()
    }
}

fn probe_prefixes(ctx: &ReadContext, id: &[u8]) -> Result<bool> {
    let prefix_len = id.len();
    loop {
        let mut key_buf = vec![keys::SUBSCRIBED_PREFIXES];
        key_buf.extend_from_slice(&id[0 .. prefix_len]);
        if try!(ctx.tx.get(&ctx.db, &key_buf).is_some() { return Ok(true); }
        if prefix_len == 0 { return Ok(false); }

        prefix_len -= 1;
        while prefix_len > 0 && id[prefix_len - 1] >= 128 { prefix_len -= 1; }
    }
}

fn bytes_to_u32(bytes: &[u8]) -> Option<u32> {
    if bytes.len() == 4 { Some(NativeEndian::read_u32(bytes)) } else { None }
}

fn bytes_to_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 8 { Some(NativeEndian::read_i64(bytes)) } else { None }
}

fn get_up_epoch(ctx: &ReadContext) -> i64 {
}

fn update_data(ctx: &WriteContext, entity_id: &[u8], operation_id: Option<&[u8]>, operation_data: &[u8]) {
    // are we missing cache data for the entity or operation?  return NOT_READY
    // do we already have the operation?  return SUCCESS
    //    have op. ID -> check operation for any state other than initial
    //    no op. ID -> check if current state is >= passed state
    // append to local operation buffer
    // apply to local state
    // apply changes in local state to local delta indices
}

fn update_clock(ctx: &WriteContext, system: &Uuid, clock: i64) {
    // max()
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
