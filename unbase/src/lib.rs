extern crate lmdb;

use lmdb;

// for the second pass, our data is stored in an LMDB object
// LMDB keys:
// #1 : general bookkeeping data
// #1 #1 downstream-id #1 counter : pending messages
// something about subtree subscriptions?
// #2 : data per object that is tracked in the system
// #2 object-id : upstream data (must exist, we are an inclusive cache), our version if different
// individual subscriptions go here?
// #3 : data per index (upstream or downstream?? not sure)

// TODO: why does MdbValue exist?  I'd just make it a rust slice
// TODO: lmdb-rs uses trait objects too much

struct UnbaseEngine {
    env: lmdb::Environment,
}

#[test]
fn it_works() {
}
