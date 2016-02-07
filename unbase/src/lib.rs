#![allow(dead_code)]
extern crate lmdb;
extern crate uuid;
extern crate capnp;
extern crate byteorder;

mod unbase_capnp {
    include!(concat!(env!("OUT_DIR"), "/unbase_capnp.rs"));
}
use lmdb::{RoTransaction, RwTransaction, Transaction, WriteFlags, Cursor};
use uuid::Uuid;
use byteorder::{ByteOrder, NativeEndian};

mod keys {
    pub const SIGNATURE : u8 = 1;
    pub const SIGVERSION : u8 = 2;
    pub const SYSTEM_UUID : u8 = 3;
    pub const DATABASE_UUID : u8 = 4;
    pub const SUBSCRIBED_PREFIXES : u8 = 5;
    pub const UP_STATE_BY_ID : u8 = 6;
    pub const UP_SYSTEM_UUID : u8 = 7;
    pub const UP_SYSTEM_EPOCH : u8 = 8;
    pub const THIS_SYSTEM_EPOCH : u8 = 9;
    pub const KNOWN_CLOCKS : u8 = 10;
    pub const SUBSCRIBED_NAMES : u8 = 11;
    // CHANGE_xyz are never committed, temp storage during update pass
    pub const CHANGE_CLOCK : u8 = 12;
    pub const LATTICE_DELTA_BY_ID : u8 = 13;
    pub const LATTICE_DELTA_EPOCH : u8 = 14;
    pub const CHANGE_LATTICE_DELTA : u8 = 15;
    pub const OPS_BY_OP_ID : u8 = 16;
    pub const OPS_BY_ENTITY_ID : u8 = 17;
    pub const OPS_BY_ENTITY_ID_EPOCH : u8 = 18;
    pub const CHANGE_OP_DELTA : u8 = 19;

    pub const SIGNATURE_VALUE : &'static str = "Unbase/T database";
    pub const SIGVERSION_VALUE : u32 = 0x10000;
    pub const ID_INVALID : u8 = 0x80;
}

mod binding {
    pub fn lattice_edit(_key: &[u8], _up_val: &[u8], _change: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn op_edit(_value: &[u8], _op: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn lattice_combine(_key: &[u8], _ed1: &[u8], _ed2: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn default_state(_key: &[u8]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn deleted() -> Vec<u8> {
        unimplemented!()
    }
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
    fn from(_e: lmdb::Error) -> Error {
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

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: &K, val: &V) -> Result<()> {
        try!(self.tx.put(self.db, key, val, WriteFlags::empty()));
        Ok(())
    }

    // TODO upstream
    fn get_opt<K: AsRef<[u8]>>(&self, key: &K) -> lmdb::Result<Option<&[u8]>> {
        match self.tx.get(self.db, key) {
            Ok(val) => Ok(Some(val)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

fn probe_prefixes(ctx: &WriteContext, id: &[u8]) -> Result<bool> {
    let mut prefix_len = id.len();
    loop {
        let key = vec2![keys::SUBSCRIBED_PREFIXES, ...&id[0 .. prefix_len]];
        if try!(ctx.get_opt(&key)).is_some() { return Ok(true); }
        if prefix_len == 0 { return Ok(false); }

        prefix_len -= 1;
        while prefix_len > 0 && id[prefix_len - 1] >= 128 { prefix_len -= 1; }
    }
}

fn has_up_system(ctx: &WriteContext) -> Result<bool> {
    let val = try!(ctx.get_opt(&[keys::UP_SYSTEM_UUID]));
    Ok(val.is_some())
}

fn probe_name(ctx: &WriteContext, id: &[u8]) -> Result<bool> {
    if !try!(has_up_system(ctx)) {
        return Ok(true); // root system has all
    }

    if try!(ctx.get_opt(&vec2![keys::SUBSCRIBED_NAMES, ...id])).is_some() { return Ok(true); }
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
    let epoch = ctx.current_epoch;
    try!(ctx.put(&vec![keys::THIS_SYSTEM_EPOCH], &i64_to_bytes(epoch)));
    Ok(())
}

enum UpdateDataResult {
    NotReady,
}

fn get_computed_state(ctx: &WriteContext, entity_id: &[u8]) -> Result<Option<Vec<u8>>> {
    if !try!(probe_name(ctx, entity_id)) {
        return Ok(None)
    }

    let mut up_state = match try!(ctx.get_opt(&vec2![keys::UP_STATE_BY_ID, ...entity_id])) {
        Some(upref) => upref.to_owned(),
        None => binding::default_state(entity_id),
    };
    if let Some(ldelta) = try!(ctx.get_opt(&vec2![keys::LATTICE_DELTA_BY_ID, ...entity_id])) {
        up_state = binding::lattice_edit(entity_id, &up_state, ldelta);
    }
    if try!(ctx.get_opt(&vec2![keys::OPS_BY_OP_ID, ...entity_id])).is_some() {
        up_state = binding::deleted();
    }
    let op_prefix = vec2![keys::OPS_BY_ENTITY_ID, ...entity_id, keys::ID_INVALID];
    {
        for (kref, vref) in try!(ctx.tx.open_ro_cursor(ctx.db)).iter_from(&op_prefix) {
            if !kref.starts_with(&op_prefix) {
                break;
            }
            up_state = binding::op_edit(&up_state, vref);
        }
    }

    Ok(Some(up_state))
}

macro_rules! unwrap_or {
    ($opt:expr, $els:stmt) => {
        match $opt {
            Some(val) => val,
            None => { $els },
        }
    }
}

fn update_data(ctx: &mut WriteContext, entity_id: &[u8], operation_id: Option<&[u8]>, operation_data: &[u8]) -> Result<bool> {
    let epoch = ctx.current_epoch;
    match operation_id {
        Some(op_id) => {
            let opcstate = unwrap_or!(try!(get_computed_state(ctx, op_id)), return Ok(false));
            // do we already have the operation?
            if opcstate != binding::default_state(op_id) { return Ok(true); }
            // we can't accept an operation unless we already have the object (need to be able to fully predict the index change)
            if !try!(probe_name(ctx, entity_id)) { return Ok(false); }
            // record it as a change
            try!(ctx.put(&vec2![keys::OPS_BY_OP_ID, ...op_id], &[])); // TODO use in get_computed_state
            try!(ctx.put(&vec2![keys::OPS_BY_ENTITY_ID, ...entity_id, keys::ID_INVALID, ...op_id], &operation_data)); // TODO use in get_computed_state
            try!(ctx.put(&vec2![keys::OPS_BY_ENTITY_ID_EPOCH, ...entity_id, keys::ID_INVALID, ...op_id], &i64_to_bytes(epoch)));
            // propagate
            try!(ctx.put(&vec2![keys::CHANGE_OP_DELTA, ...op_id, keys::ID_INVALID, ...entity_id], &operation_data));
            Ok(true)
        }
        None => {
            let cstate = unwrap_or!(try!(get_computed_state(ctx, entity_id)), return Ok(false));
            let nstate = binding::lattice_edit(entity_id, &cstate, operation_data);
            // check if current state dominates operation_data
            if nstate == cstate {
                return Ok(true);
            }
            // append to local operation buffer
            let nchange = match try!(ctx.get_opt(&vec2![keys::LATTICE_DELTA_BY_ID, ...entity_id])) {
                None => operation_data.to_owned(),
                Some(ochval) => binding::lattice_combine(entity_id, ochval, operation_data),
            };
            // apply to local state
            try!(ctx.put(&vec2![keys::LATTICE_DELTA_BY_ID, ...entity_id], &nchange));
            try!(ctx.put(&vec2![keys::LATTICE_DELTA_EPOCH, ...entity_id], &i64_to_bytes(epoch)));
            // TODO apply changes in local state to local delta indices
            // propagate this change up/sideways
            try!(ctx.put(&vec2![keys::CHANGE_LATTICE_DELTA, ...entity_id], &nchange));
            Ok(true)
        }
    }
}

fn update_clock(ctx: &mut WriteContext, system: &Uuid, clock: i64) -> Result<()> {
    let clock_key = vec2![keys::KNOWN_CLOCKS, ...system.as_bytes()];
    let old_clock = try!(ctx.get_opt(&clock_key)).and_then(bytes_to_i64).unwrap_or(0);
    if clock > old_clock {
        try!(ctx.put(&clock_key, &i64_to_bytes(clock)));
        try!(ctx.put(&vec2![keys::CHANGE_CLOCK, ...system.as_bytes()], &i64_to_bytes(clock)));
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

#[test]
fn it_works() {
}
