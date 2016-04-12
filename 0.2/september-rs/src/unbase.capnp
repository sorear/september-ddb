@0x877321e82eb610b5;

struct ClockUpdate {
  system @0 :Data; # uuid
  newEpoch @1 :UInt64;
}

struct StateUpdate {
  objectId @0 :Data;
  attributeId @1 :Data; # null for delete?  or a system-defined ID?
  value @2 :Data; # may warrant structure.  Unclear.
}

struct Operation {
  objectId @0 :Data;
  operationId @1 :Data; # null for state-based CRDTs
  attributeId @2 :Data; # null for delete?  or a system-defined ID?
  value @3 :Data; # may warrant structure.  Unclear.
}

struct IndexUpdate {
  # rather unclear what should go here for the more general types of index
  attributeId @0 :Data;
  value @1 :Data;
  addedObjectIds @2 :List(Data);
  removedObjectIds @3 :List(Data);
}

struct ReplicateDownMessage {
  newEpoch @0 :UInt64;
  clockUpdates @1 :List(ClockUpdate);
  includesTo @2 :UInt64; # recipient's epoch
  stateUpdates @3 :List(StateUpdate);
  indexUpdates @4 :List(IndexUpdate);
}

struct ReplicateUpMessage {
  newEpoch @0 :UInt64;
  operations @1 :List(Operation);
}

struct ReplicateSiblingMessage {
  referenceSystem @0 :Data;
  referenceEpoch @1 :UInt64;
  clockUpdates @2 :List(ClockUpdate);
  operations @3 :List(Operation);
}
