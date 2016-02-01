extern crate lmdb;
extern crate uuid;
use unbase_capnp;

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

// TODO(suggestion): add as_ro() to RwCursor and RwTransaction ?

struct UnbaseEngine {
    env: lmdb::Environment,
}

fn epoch_bump_index(tx: &mut lmdb::RwTransaction) {
}



#[test]
fn it_works() {
}
