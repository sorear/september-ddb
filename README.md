# Unbase/T (topounbase)

For the manifesto, see https://github.com/dnorman/unbase.

topounbase is not an unbase by pedigree, but it ticks so many of the same boxes that I suspect it is cryptomorphic.

topounbase (which needs a better name) is a distributed database which ignores much of the literature on distributed databases in favor of the literature on cache coherency protocols for shared-memory multiprocessors: a memory bank is a kind of database, and the memory controller people have been studying consistency under load for a long time.

topounbase can also be seen as a "NoSQL database" which treats XDCR and frontend caching as first-class citizens and gives them a sane consistency model.

topounbase achieves causal+ consistency without tracking dependency metadata or requiring all systems to see all updates.
As such it defies the assumptions and conclusions of [The Potential Dangers of Causal Consistency and an Explicit Solution](http://db.cs.berkeley.edu/papers/socc12-explicit.pdf).

## How it works

topounbase describes the "universe" as a tree of "systems", each of which is a writeback cache to its parent system.

The root system holds all data and receives all write traffic.
This would be a problem, except that systems are allowed to themselves be (CP) distributed clusters.
CP distributed systems as a rule experience high write latency; use of geographically small writeback caches hides this latency.

For a read to see a write, it must propagate to an enclosing system.
Thus propagation times can roughly follow network latency, although we assume that your network is described by an **ultrametric**.
(Support for non-ultrametric networks is something I've been struggling with for a while.  I still hold some hope.)

There may be a critical scale at which it makes sense to have multiple full copies of the data;
we support multiple root systems for this reason.

Each system which is not a root contains a subset of the data, and a subset of the data which is held by its parent system.
The parent system knows the subset, and can propagate changes;
each system maintains a vector clock representing its causal cut, and all of its held data is up to date for that cut.

Barrier (acquire and release) instructions, by analogy with the corresponding concepts in SMP, can be used to wait for one system's cut to be as up to date as another.
This is required for RPC without anomalies and as such is built in to the system.

Maintaining causal order in the presence of parallel replication is hard.
topounbase's critical insight here is that while you cannot safely reorder updates without metadata,
you can forget which update came first by bundling them into a transaction,
and bundling transactions into ever-larger transactions as they ascend the cache hierarchy so as to maintain the transaction rate below the inverse light-diameter of the cache.

As progressively larger caches can thus be seen to tick slower and slower, topounbase can also be seen to maintain causality by partitioning spacetime into spherical cells of various sizes.
I've toyed with a metaphor of "frequency-domain data replication" but it has not seemed fruitful so far.

topounbase draws a distinction between that which is replicated up (currently "data", although "commands" might be more apt in some cases) and that which is replicated down ("indices").
Data cannot be interpreted in isolation; indices always require higher-level indices for construction, but are modified by data at each level.
To support reading newly created data without requiring a round trip to the root system, the object ID space is partitioned and each system pre-receives a subscription to its portion of the ID space.
Data replication behaves as a state-based CRDT.
Support for operational CRDTs is planned but will probably require audit log integration, as sideways replication would otherwise allow multipathing.

## Technical risk items

In descending order of riskiness.

* Distribution: While the scheme has been designed to minimize the logical depth of operations required to process each request,
that does not guarantee that implementing a system as a distributed cluster will actually work.
If it doesn't work, that derails this whole plan.

* Linearizable operations: We intend to offer these, although they will not be the default.
They are largely not even designed.
Current thought is that a linearizable operation must be explicitly targeted at a single system, and may reach it by way of RPC.

* View system: The hard part is not calculating the views, but establishing the precise consistency rules that should apply to them.
Updating a view will take time and if a system maintains views,
it should not wait for the views to compute before propagate non-view changes to downstream systems.
Thus we would like replicated views to be able to explicitly indicate an older version, which probably requires an additional set of barriers to manage them.}"}"
There are many details to work out here.

* Eviction: This is clearly needed for a functioning cache hierarchy but has barely been designed.
Since topounbase uses inclusive caching, an eviction at an intermediate level forces lower levels to evict;
this could prove problematic for anything that has outstanding read requests.

* Modularity: Especially as the view system grows, the functionality associated with a system will become quite large.
Can we, in any meaningful way, declare the view system to be "not part of topounbase" and firewall its complexity?

* Deletion: Deleting data with tombstones is easy enough.
It is not clear how we could decide when tombstones can safely be removed.

* Security: We would like to support various kinds of partially-trusted system and various kinds of sensitive/compartmentalized data.
Design work here has barely begun.

* Operational CRDTs, the audit log, and explicit indication of crossed writes: This is largely designed and expected to work.

* Removing the ultrametric limitation: I'm not sure this is possible, but we can live without doing it.
This is closely tied to the behavior of sibling replication.
