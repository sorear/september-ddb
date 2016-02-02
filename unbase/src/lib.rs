extern crate lmdb;
extern crate uuid;
extern crate capnp;

mod unbase_capnp {
    include!(concat!(env!("OUT_DIR"), "/unbase_capnp.rs"));
}
use lmdb::{RoTransaction, Transaction};

mod keys {
    pub const SIGNATURE : u8 = 1;
    pub const SIGVERSION : u8 = 2;
    pub const SYSTEM_UUID : u8 = 3;
    pub const DATABASE_UUID : u8 = 4;
    pub const SUBSCRIBED_PREFIXES : u8 = 5;
    pub const STATE_BY_ID : u8 = 6;

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

struct ReadContext {
    tx: lmdb::RoTransaction,
    db: lmdb::Database,
}

fn query_state(ctx: &ReadContext, id: &ObjectId) -> Result<ObjectState> {
    let key = vec![keys::STATE_BY_ID];
    key.extend(id.0);
    match try!(tx.get(&ctx.db, &key)) {
        Some(data) => Ok(ObjectState::Exists(data.to_owned())),
        None => {
            // loop over prefixes and query them
        }
    }
}

fn epoch_bump_index(tx: &mut lmdb::RwTransaction) {
}

#[test]
fn it_works() {
}
