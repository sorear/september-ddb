# September design document (3rd try)

In which I do the design before the code.

## Concepts

### Cache coherency

### Directories

### Fences

### Speculation

### Identifiers

### A message-oriented protocol

Message delivery is assumed to be reliable but out-of-order to allow for
sharding.  All messages are idempotent.

## Data model

## Messages

### Managing identifiers

#### GrantSystemId(id)

Sent downstream after initial connection establishment to provide the new system
its permanent ID.

#### GrantArc(id-prefix)

Sent downstream to provide an arc for allocating object, operation, and system
IDs.  Any lesser arc which has previously been granted is considered revoked,
and the downstream must reply with a ReleaseArc for the lesser arc as soon as
possible.

TODO: "lesser" is strange here, but explicit revocation would be difficult to
make idempotent.  Is there a better way?

#### ReleaseArc(id-prefix, epoch)

Sent upstream as a promise that the id-prefix will not be used for any new
object or operation in epochs later than the one indicated.  System IDs are not
allocated in epochs and may continue to be assigned.

### Epoch management

An epoch bundles a set of messages which occur at the same logical time, and may
not be split.  Epoch handling is a measured departure from the
unordered-delivery principle, and requires special handling on unordered links,
for instance by sending BeginUpdate / EndUpdate on each independent channel and
linking them.

TODO: decide linking mechanism.

#### BeginUpdate(epoch)

Indicates the start of updates for an epoch.  Only applicable on strongly
ordered links.

#### EndUpdate

Indicates the end of updates for an epoch.  Only applicable on strongly ordered
links.

### NullUpdate(epoch)

Explicitly indicates that no updates are applicable between the most recent
epoch sent and the current epoch, which is indicated.  Since update filtering
only occurs in the downstream direction, this is only sent downstream; it is not
sent in all cases, but in response to WaitForEpoch that would otherwise
deadlock, and proactively with Grant if needed.

### Index handling

#### Acquire(index-spec)

Sent upstream when a system needs the value of an index which it does not
currently possess.  The upstream will synthesize the index (possibly by passing
an Acquire to its upstream), and respond with a Grant.

Read transactions are handled using an alternate version of Acquire that takes
an epoch, and sampling epoch at the beginning of the transaction.  It may be
necessary to add an epoch coherence mechanism to avoid bad interactions with
NullUpdate.

#### Grant(index-spec, min-epoch, max-epoch, value)

Sent downstream in response to an Acquire.  This will be current, except in read
transaction scenarios.  The upstream should send an update for max-epoch + 1
containing a Subscribed message, but if it doesn't that should be taken as an
implicit Invalidate (this allows the upstream to lose Acquires, which allows
them to be handled without durable storage).

#### Invalidate(index-spec)

Sent in an update to indicate that the index has changed and any prior
Subscribed is no longer valid.

TODO: delta updating results in lower latency, but requires caution to bound the
resulting traffic.

#### Subscribed(index-spec)

Sent in an update to indicate that the value provided for a Grant in the
immediate previous epoch is still valid, and will continue to be valid until an
Invalidate is sent.

### Memo handling

#### PublishOp(operation-id, object-id, delta)

Sent upstream or sideways as part of an update to indicate changes to original
data.  The operation-id is a unique deduplicating identifier, and must fall
under the currently active GrantArc for the originating system; the object-id is
not so constrained, but as it is intended to identify a data storage granule it
would be good to avoid excessive publishes against a single object-id.  Flow
control quotas will almost certainly be implemented.   The format of the delta
is unspecified at this time, but it must be possible to compute the same index
values from any causally-consistent order of the deltas.

### Things that UserMessages can replace

#### FollowsEpoch(my-epoch, other-system, other-epoch)

#### NotifyReplicated(your-epoch, other-system, other-epoch)

#### WaitForEpoch(other-system, other-epoch)

#### Announce(system-id, service-info)

#### Deannounce(system-id, service-info)

#### Connectivity(system-id, affects-all-services)

### User messaging

#### UserMessage(system-id, causal-context, message-type, payload)

A message which is forwarded along the system tree until it reaches a target,
while not outpacing the causal context (which is an OR-constraint of system IDs
and epochs).

TODO: this is not idempotent

TODO: should this be able to cross roots?
