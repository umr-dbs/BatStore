# cMVBT index optimizations

This document is a compact guide to the optimizations used by the current cMVBT index.
It describes the resulting design, not the order in which it was developed. Correctness
always takes priority: an optimization is used only where the MVCC, OSIC, optimistic-lock
coupling (OLC), and reclamation invariants still hold.

## At a glance

| Area | Straightforward design | cMVBT design | Main effect |
|---|---|---|---|
| Leaf layout | Array of complete records | Dense key array plus parallel version/payload array | More keys per cache line during lookup |
| Invalid entries | Read every version header | Inline 128-bit validity mask | Reject aborted slots before loading record data |
| Page allocation | Size only the `Block` | Size `OptCell<Block>`, including `cell_version` | The normal allocation is exactly 4 KiB |
| Reads | Latch every node | OLC traversal with version validation | Readers normally take no node latch |
| Root lookup | Re-search root history | Cache the transaction's historical read root | Repeated reads avoid repeated root-index lookup |
| Visibility | Search all commit logs repeatedly | Per-thread, per-database snapshot cache | Reuses visibility-search state |
| Transaction metadata | Heap `Vec`s | Inline `SmallVec`s for tables and write set | Common transactions allocate nothing |
| Repeated updates | Append every intermediate value | Reuse a transaction-owned version; optional safe update-in-place | Less version-chain growth and later GC work |
| Range scans | Materialize vectors | Zero-copy, streaming leaf iteration | Lower allocation and copying cost |
| Reclamation | One global free list | Snapshot-safe, sharded reuse with stealing | Less allocator and global-lock contention |
| WAL disabled | Optional pointer checked dynamically | Construction-time `WalBackend::Off` enum variant | Cheap, immutable no-WAL fast path |
| WAL enabled | One serialized append path | Sharded group commit or reserved-offset batched writes | Less enqueue/syscall contention |

“Main effect” identifies the removed cost; it is not an independent end-to-end speedup
claim. Workload-level gains can overlap and must be measured together.

## Pages and memory layout

### Structure-of-arrays leaves

A leaf stores keys in one dense region and the remaining data at the same indexes:

```text
key_region:   [key0, key1, key2, ...]
data_region:  [(version0, payload0), (version1, payload1), ...]
```

Point lookup walks the key region backward because physical append order places the newest
matching version last. Version metadata and payload cache lines are touched only after a
key matches. A scan can likewise test its key range before loading the parallel data.
Borrowed record views preserve zero-copy iteration without reconstructing an array of full
records.

For `u64` keys and the one-word payload slot, a physical entry remains 32 bytes: 8 bytes
of key plus 24 bytes of version and payload data. The layout improves locality without
changing leaf capacity. The isolated multi-leaf benchmark measured point-lookup gains of
8.5–22.1%, depending on how many historical versions had to be examined; full scans were
effectively unchanged. See [the leaf-layout benchmark](leaf_layout_benchmark.md).

### Inline validity mask

The normal leaf has at most 123 physical slots, so two `AtomicU64`s cover every slot. These
128 bits are stored directly in the page. A clear bit means that the slot was invalidated,
for example by transaction abort, allowing lookup to skip it before loading its version
header. This is a **physical-validity mask**, not an MVCC visibility mask: visibility also
depends on the querying transaction's snapshot and cannot be represented by one universal
bit per slot.

The two words occupy the same 16 bytes previously required by a boxed-slice handle and
remove the normal leaf's extra allocation and pointer chase. Deliberately oversized TPC-C
leaf variants use a heap bitmap through a same-sized union representation.

### Actual 4 KiB allocation

The page-size target applies to the allocated `OptCell<Block<...>>`, not merely to `Block`.
This includes the OLC `cell_version`. With `FAN_OUT = 123` and `NUM_RECORDS = 123`, the
normal cell is exactly 4096 bytes; 124 records cross that boundary. A regression test
checks both facts. The sizing produces page-aligned allocator objects and avoids routinely
straddling two physical pages.

Internal pages already use separate key-interval, version, and child-pointer regions.
Child pointers and immutable published metadata are plain slots: the page-length Release/
Acquire publication edge makes initialized contents visible, so redundant per-slot atomics
are avoided.

### Payload indirection only when required

`PayloadSlot` occupies one machine word. A one-word payload such as `u64` or the thin YCSB
row handle is stored inline. Larger TPC-C rows are held through a reference-counted pointer.
Cloning a read result or copying a survivor during a split therefore increments a reference
count instead of copying a large row. Replacement installs a new value rather than mutating
shared storage.

## Traversal and synchronization

### Optimistic lock coupling

Readers sample each node's version, inspect immutable or atomically published contents,
and validate the version before relying on the result. They retry if a writer changed or
retired the node. Writers acquire exclusive node guards only where mutation is required.
This removes reader latch traffic while retaining a precise stale-node check.

Memory ordering is kept on the publication edges: initialized slots precede a Release
length update, and readers acquire the length before indexing. Independent counters and
diagnostics use Relaxed ordering where they carry no publication responsibility.

### Root-path work

Root history supports historical snapshots, while current OLTP traversal uses the newest
safe root. A database transaction caches each table's resolved historical read root, so
multiple operations on the same table and snapshot do not repeatedly traverse the root
index. Root objects remain pinned through the existing block references; cache use does not
replace OLC validation of traversed nodes.

### Efficient structural maintenance

Splits use available page capacity before triggering structural work. Leaf splitting
prefers a nearby logical-key boundary whose two sides fit; if compaction leaves exactly a
full page, it performs a key split because the pending write still needs a slot. Merges
consider the emptier adjacent sibling and count the actual GC survivors before deciding
that the combined page fits. Record append order is preserved so reverse newest-version
lookup remains valid.

Node reuse reconstructs the active union member when switching between leaf and internal
page types. Same-type reuse clears only the required state and retains reusable allocation
where safe.

## MVCC, OSIC, and transaction-local work

### Snapshot and visibility caches

Worker identity is thread-local, avoiding repeated registration lookup. Each thread keeps
two common per-database `SnapshotCache` instances inline and spills to a vector only when
it touches more databases. Each cache is sized from that database's runtime worker count,
so there is no compile-time maximum-worker array embedded in every cache.

Visibility checks reuse the cached commit-log position for a worker instead of restarting
the search for every version. Snapshot registration remains gap-free: a reader is visible
to GC before reclamation can pass its timestamp. Traversals that only need a fresh start
timestamp draw one without creating a second, unnecessary snapshot registration.

### Compact transaction state

The common write set is an inline `SmallVec` of `(table slot, key)` pairs, and the common
set of touched tables is inline as well. Small transactions therefore avoid heap allocation.
Abort processes the write set in reverse order and batches adjacent writes to the same key,
allowing all versions produced by a transaction to be reverted under one leaf latch.

Transaction-owned mutable state uses thread-confined interior mutability rather than
runtime borrow checking: only the transaction's owning thread can execute it.

### Avoiding unnecessary versions

Repeated updates by the same open transaction overwrite its own newest version rather than
appending every intermediate value. Delete/reinsert cycles can reuse that same owned slot.
When GC proves that no live snapshot can observe the predecessor, the optional
update-in-place path can replace a committed payload without extending the version chain.
That latter shortcut is disabled where the selected WAL representation cannot describe it
safely.

## Scans, garbage collection, and allocation

Range queries stream borrowed leaf records instead of building intermediate vectors.
Bounded scans test keys before visibility, and count/fold/for-each APIs allow analytical
queries to consume rows without materializing payload copies. Traversal follows ordered
leaf routing and revalidates OLC state across structural changes.

GC uses registered oldest snapshots to retain precisely the history readers and abort may
still need. Reusable blocks are sharded to reduce contention; a depleted shard can steal
from another shard rather than allocate immediately. Retirement is encoded in the cell's
version state so a stale reader cannot successfully upgrade a reference to an orphaned
node.

TPC-C optionally gives its tiny, hot Warehouse and District tables larger leaf classes.
This reduces frequent version compaction and root restarts without inflating every table.
It is an explicit tradeoff: larger leaves retain more dead versions and can make aged
analytical scans slower. See [the big-tree benchmark](bigtree_size_benchmark.md).

## WAL and build-time code generation

WAL selection is immutable after tree construction. `WalBackend::Off` makes the in-memory
case an enum fast path without `ArcSwapOption` or runtime reconfiguration machinery. The
durable configurations offer either sharded channel-based group commit or lock-free offset
reservation with per-thread batching. WAL records use a table-driven CRC32 implementation,
and durability watermarks account for writers completing out of order.

Release builds use the configured native CPU target, allowing the compiler to specialize
hot comparison, atomic, checksum, and copying code for the benchmark machine. Cross-machine
results must therefore use equivalent build settings. Detailed measurements and caveats are
in [the OLTP/WAL optimization report](oltp_wal_optimization.md).

## Complete current optimization inventory

The following tables are the detailed checklist of optimizations currently used by the
index and its transaction/database layer. They intentionally repeat a few important items
from the explanation above so that this section can be used independently during review.

### Object layout and allocation

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| Split leaf arrays | Dense `[Key]` plus parallel `[VersionInfo, PayloadSlot]` | Unrelated version/payload cache-line traffic during key rejection | Both regions must always use the identical physical index |
| Reverse latest-version lookup | Search append order from newest to oldest | Avoids sorting or building a per-key index | Physical append order must be preserved by split, merge, and compaction |
| Inline leaf validity bits | `[AtomicU64; 2]` for up to 123 normal slots | One allocation and mask pointer chase per normal leaf | The bits mean “not invalidated,” not snapshot-visible |
| Oversized-mask fallback | Same-sized union stores a boxed bitmap above 128 slots | Keeps large experimental TPC-C leaves supported without enlarging normal leaves | Correct union construction/drop is required on page-type reuse |
| Exact allocation sizing | `OptCell<Block<123, ...>> == 4096` bytes | Page straddling and allocator size-class waste | Must include `cell_version`, node metadata, and alignment—not only record arrays |
| Cache-line-aligned node | Hot node/latch state starts on a 64-byte boundary | False sharing with neighboring allocations | Padding is part of the page-capacity calculation |
| One-word payload slot | Inline word-sized payloads; `triomphe::Arc` for larger rows | Large-row copies during reads, splits, and scans | Shared payloads are replaced, never mutated through an alias |
| Thin YCSB row handle | Length/refcount live in the row allocation, handle is one word | A second boxing layer and fat-pointer storage | Whole-row replacement preserves immutable shared buffers |
| Zero-initialization avoided | Page arrays use `MaybeUninit` and publish only initialized prefixes | Initializing every unused slot on page creation/reuse | `len` publication and exact drop counts are safety-critical |
| Same-type page reuse | Clear lengths/masks/pointer state and reuse the allocation | Reallocation and allocator traffic | Initialized payload slots must be dropped exactly once |
| Type-changing page reuse | Drop active union member and construct the destination member | Unsafe bitwise reinterpretation and leaked/double-dropped mask storage | The node tag and union member must change together under exclusivity |
| Inline database table list | `SmallVec` inside the atomically published table-list `Arc` | Second pointer chase from `Vec` header to element buffer | It spills safely if the generous inline table capacity is exceeded |
| Direct table IDs | Sequential `TableId` indexes the table slice | Hashing/name lookup on every transaction operation | Catalog recovery must recreate the same table order |
| Per-table trees | Independent root, leaves, and allocator for each relation | Cross-table page/latch/SMO contention | Cross-table atomicity is provided by shared transaction context and WAL |
| Hot/cold `MVBTSt` split | Root, allocator, and transaction context remain hot; increment/decrement functions, bounds, WAL, and table ID sit behind `cold: Box<_>` | Cache footprint of traversal-critical tree fields | Construction-only configuration must remain immutable or cold |

### OLC, publication, and traversal

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| Latch-free readers | Seqlock-style `cell_version` sample and validation | Reader lock acquisition and reader/writer cache-line writes | Every unlatched observation must be immutable, atomic, or protected by publication ordering |
| Writer-only exclusive guard | Writers upgrade only the node being changed | Hand-over-hand exclusive locking | Ancestors and selected children must still be checked for retirement/change |
| Retirement bit in latch word | Retired state is encoded in `cell_version` | A separate atomic and a race between retirement and guard drop | Unlock/version increments must preserve state bits |
| Direct retirement where final | A consumed child guard can retire without a reversible lock upgrade | One CAS/upgrade during eligible SMOs | Only valid when the path cannot back out and reuse the child |
| Length publishes slots | Slot writes happen before Release length update; readers load length with Acquire | Per-slot atomics for immutable key, payload, version, and child-pointer storage | No reader may index beyond its acquired prefix |
| Plain internal versions/pointers | Published internal metadata is written once | Atomic load/store overhead per internal entry | Later in-place mutation would invalidate the proof |
| Derived internal liveness | Select live, disjoint intervals instead of mutating an obsolete flag | One atomic field and obsolete-marking protocol | Live sibling intervals must remain disjoint |
| Minimal fences/orderings | Acquire/Release only on real publication edges; Relaxed for independent counters | Redundant fences and global ordering | Weakening an actual ownership/publication edge is not allowed |
| No master write-traversal guard | Root and node atomic state replace a global traversal guard | Global writer serialization | Root selection and unsafe-root handling remain validated |
| Current-root fast path | OLTP write traversal begins from the newest safe root | Historical root-index predecessor search on ordinary writes | An unsafe root forces the existing SMO/retry path |
| Cached historical read root | One `read_root` per touched transaction/table | Repeated `root_for(ts_start)` lookup across point and range reads | Root history is append-only and cached roots stay pinned |
| Runtime root-index choice | Frugal list, linked list, skip list, or B-tree | Lets deployments match append/lookup cost to root-history size | Every implementation must agree on predecessor boundary semantics |

### Clock, OSIC, snapshots, and visibility

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| One global logical clock | Start and commit timestamps both come from one `AtomicVersion` | Separate timestamp domains and publish protocol | Atomic modification order gives unique, globally ordered values |
| Relaxed clock increment | `fetch_add(1, Relaxed)` draws timestamps | `SeqCst` fence/global-order cost | Correct because the clock orders numbers only; commit-log mutexes and snapshot Release/Acquire operations publish data |
| Per-worker monotonicity | A worker is serialized and every draw uses the same increasing global counter | Extra per-worker counter/check | A later draw by that worker necessarily receives a larger value despite Relaxed memory ordering |
| Runtime worker capacity | Registry and arrays are sized at construction from the actual worker budget | Compile-time `MAX_WORKERS_CAP` over-allocation or hard limit | Exceeding the configured long-lived worker count is rejected |
| TLS worker ID | Cache `(database UID, WorkerId)` per thread | Repeated registry acquisition/lookup | Stable process-unique UID prevents address-reuse aliasing |
| Two inline worker-ID slots | Two-entry MRU in a plain `Cell`, overflow only for rarer databases | Searchable fixed array and normal-path heap access | Overflow preserves identities so a thread never reacquires a second ID for one database |
| TLS snapshot cache | Each thread exclusively owns its cache | Shared cache synchronization and accidental cross-worker mutation | Closures must not re-enter the same unsafe TLS mutable access |
| Two inline snapshot-cache slots | Two database caches in `SafeCell`, lazy overflow vector | Allocation/search in the common one- or two-database case | Eviction must retain warmed state in overflow |
| SnapshotCache SoA | Separate boxed `snapshot_versions` and `lcb` arrays | Fetching 16-byte tuples when only one stream is checked/refreshed | Both arrays use foreign worker ID as the same index |
| LCB positive-hit fast path | `lcb[writer] > stamp.ts_start` returns immediately | Commit-log lock/search for already-known-visible versions | Cached LCB is valid only for the recorded reader snapshot generation |
| Lazy per-worker refresh | Refresh one foreign worker only when `snapshot_versions[worker] < reader_ts_start` | Recomputing every worker's LCB for every snapshot/version | Historical snapshots and same-worker visibility rules must remain correct |
| Same-worker visibility fast path | Compare the writer stamp directly after invalidity check | Commit-log lookup for the reader's own serialized transactions | Invalid stamps are rejected first; historical reader timestamps still bound visibility |
| Concrete scan visibility closure | Iterator receives cache and log slice, then builds a monomorphized closure locally | `dyn FnMut` indirect call up to twice per physical record | Logic must stay identical to the general checker |
| Per-worker live snapshot slots | Each worker publishes its outermost snapshot atomically | Global active-transaction mutex | Minimum-snapshot queries scan workers and use Acquire loads |
| Cache-padded worker slots | `live_tx`, nesting depth, and registration-bound atomics each occupy separate cache lines | False sharing between adjacent workers at high thread counts | Higher memory consumption per configured worker |
| Nested registration depth | Only the outermost registration writes the live slot | Repeated publication for nested scans/transactions | Begin/end calls must be paired on the owning worker |
| Gap-free registration guard | Publish a conservative `in_flight_bound` before drawing/registering the real snapshot | Global admission lock while preventing GC/pruning races | Handoff to `live_tx` must complete before clearing the bound |
| Lightweight reclamation pin | Traversal-only work pins reuse without drawing/registering a full OSIC snapshot | Unnecessary clock tick and live-transaction registration | Existing transactions already provide a sufficient older pin |
| Timestamp-only draw where sufficient | Writer traversal/query paths that only need `ts_start` use the draw callback rather than double-registering | Redundant snapshot registration | Any path that can retain/read historical pages still needs reclamation protection |
| Optional commit-log truncation | Prune only below all live and registering snapshots | Unbounded commit-history memory | A long historical reader intentionally delays truncation |

### Transaction execution and version chains

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| One database-wide transaction context | All table trees share clock, workers, snapshots, commit logs, and WAL | Per-table transaction coordination and inconsistent timestamps | Tables remain physically independent but commit as one logical transaction |
| One snapshot per transaction | Begin/register once across every table | Per-operation and per-table snapshot acquisition | Release exactly once on commit, abort, or drop |
| Inline touched-table cache | `SmallVec<[TxTableState; 8]>` stores table tree and cached read root | Repeated table lookup, root lookup, and heap allocation | Wide transactions spill safely |
| Inline write set | `SmallVec<[(table_slot, key); 16]>` | Heap allocation in common OLTP transactions | Record only successful physical writes—not conflicts or zero-affected operations |
| Compact write-set reference | Store table-cache slot instead of repeated `TableId`/tree pointer | Larger entries and repeated tree resolution during abort | Table cache cannot reorder while the transaction lives |
| Reverse abort | Walk successful writes in strict LIFO order | Incorrect intermediate resurrection and additional searches | Reverse order is part of rollback correctness for repeated same-key changes |
| Batched same-key abort | Undo adjacent same-key writes under one leaf latch | One traversal/latch per version written by the transaction | The batch must invalidate all transaction-owned entries and restore the proper predecessor |
| Thread-confined transaction mutation | `SafeCell`/plain exclusive transaction access instead of `RefCell` checks | Runtime borrow-counter operations | Transaction execution must not be shared concurrently |
| First-writer-wins | Reject extension of another unresolved same-key version chain | Unbounded pending chains and later wasted work | Conflicting transactions retry/abort instead |
| Self-overwrite | Replace payload of a version already owned by this exact transaction | O(N) physical versions from N repeated updates | Match full `TxStamp`, not only worker ID |
| Delete/reinsert reuse | Reuse the transaction-owned deleted slot and repair counters | One new version per logical cycle | WAL must still preserve the logical operation sequence/final state |
| GC-proven update-in-place | Replace committed payload when no live snapshot can see the predecessor | New tuple plus later compaction for eligible updates | Disabled with incompatible WAL mode; oldest-snapshot proof must be conservative |
| Shared payload clones | Read/split results clone the handle, not a large row body | Allocation and memcpy for large records | Whole-value replacement protects readers from aliasing mutation |
| One cross-table commit marker | Shared database WAL tags table records and commits the transaction once | Per-table commit markers and ambiguous partial commit | Recovery demultiplexes table IDs under the one transaction boundary |
| Empty read-only commit elision | A transaction with an empty write set releases its snapshot without drawing `ts_commit` or appending a commit-log entry | Clock and commit-log work for read-only transactions | The existing optional commit-timestamp result represents this case |

### Split, merge, GC, and block reuse

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| Use nearly all internal capacity | Delay overflow while one required slot remains | Premature SMOs | Pending parent insertion must always retain sufficient headroom |
| Exact-full leaf split | Survivor count equal to capacity triggers key split | Infinite retry/panic when compaction leaves no pending-write slot | Capacity comparisons must include the pending operation |
| Tiered key-boundary selection | Search nearest valid boundary, then broader fallbacks | Failed/repeated splits and needless version-chain tearing | Both output pages must fit; ideal same-key grouping is preferred |
| Emptier-sibling merge | Evaluate both adjacent candidates | Failed merges or immediate re-splits from always choosing one side | Candidate must be revalidated before mutation |
| True-survivor preflight | Count records that the actual copy will retain | Fixed-array overflow during merge/compaction | Preflight and copy predicates must remain identical |
| Abort-safe survivor retention | Preserve predecessors required by unresolved deletes | Losing the record needed to undo an abort | Retention can end only after the transaction outcome is known |
| Historical block routing | Old snapshots continue through historical roots/blocks | Copying every old-reader-visible row into replacement pages | Retired blocks cannot be reused before their last routed reader exits |
| Atomic insertion/deletion stamps | Lock-free readers observe indivisible transaction stamps | Data races/torn stamps without reader locks | Page publication still carries surrounding payload initialization |
| Sharded retired-block index | Worker-specific retired queues/maps | One global reclaim lock/index hotspot | Identity includes address when death versions collide |
| Own-shard-first reuse | Search local shard, steal round-robin on miss | Touching every shard for every allocation | Imbalance can delay reuse until stealing occurs |
| Nonblocking reuse fallback | Allocate fresh when reuse metadata cannot be acquired safely | Allocator/reclamation lock cycles and deadlock | Temporary memory use may rise under contention |
| Table-specific large leaves | Larger Warehouse/District leaves only | Frequent hot-root version compaction without bloating every table | Larger leaves retain more garbage and slow aged scans |

### Range and analytical scan paths

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| Lazy range iterator | Traverse/refill on demand | O(result-size) eager materialization | Iterator-owned snapshots release on exhaustion and Drop |
| Borrowed leaf views | `LeafRecordRef` reconstructs a view from parallel regions | Copying/rebuilding complete physical records | View lifetime cannot outlive the protected page access |
| Ordered cursor routing | At each internal node choose newest visible interval containing cursor | Child vectors, sorting, hash deduplication, and duplicate scan results | Cursor advancement must handle maximum key and SMOs correctly |
| One visibility-cache access per refill | Wrap all leaves visited by one refill in one TLS cache borrow | TLS lookup/closure construction per leaf | Snapshot and worker remain fixed throughout refill |
| Key-before-visibility filtering | Reject out-of-range keys before insertion/deletion visibility checks | Up to two LCB checks for irrelevant rows | Both predicates must preserve identical accepted-row semantics |
| Full-domain specialization | Exact min-to-max scans omit per-record bound checks | Two comparisons per physical row in Q1/Q6-style scans | Only exact full-domain ranges may use it |
| Leaf-at-a-time buffer | Extend one `VecDeque` with a leaf's matches | Per-row traversal state reconstruction | Result order follows physical leaf output, not key sorting within a leaf |
| Streaming reference visitors | `for_each_ref`, `fold_ref`, and fallible visitor variants | Result objects and payload-handle clones | Callback-scoped references cannot be retained or re-enter unsafe tree access |
| Specialized count | Count visible rows without constructing `RecordPointResult` | Payload cloning and result allocation | Routing and visibility must exactly match normal iteration |
| Early-exit minimum | Compare only matching records from the first relevant leaf | Materializing/sorting the entire range | Leaves are append-ordered, so every match in that leaf must be compared |
| Transaction-root iterator constructor | `new_with_root` accepts the cached snapshot root | Repeated historical root resolution | Transaction owns the snapshot registration; iterator must not release it |

### WAL, database publication, and benchmark hot paths

| Optimization | Implementation | Cost removed or bounded | Important constraint |
|---|---|---|---|
| Immutable WAL backend | `Arc<WalBackend>` selected at construction, including `Off` | `ArcSwapOption` load/configuration machinery on every operation | Runtime backend switching is intentionally unsupported |
| Explicit no-WAL variant | Direct enum match for `WalBackend::Off` | Optional-pointer indirection and allocation | Update-in-place eligibility can use this immutable fact |
| Nonblocking group-commit enqueue | Producers send records; background workers batch/harden | File I/O on transaction threads | Queue growth/backpressure and durability notification remain correct |
| Four producer shards | Worker ID selects channel/flusher | Shared-channel producer spin contention | Durability cannot be represented by one naive maximum across shards |
| Group-commit linger | Briefly collect more pending records per hardening | Excess write/fsync calls | Throughput/latency tradeoff is configurable |
| Per-shard submitted/confirmed state | Aggregate only work actually submitted by each active shard | False durability from out-of-order flush completion | Submission publication must precede enqueue |
| Ticket-free normal logging | Production calls do not allocate an acknowledgment channel | Per-record channel allocation | Explicit ticket APIs remain for callers that truly wait for one record |
| Table-driven CRC32 | Lookup-table checksum rather than bit-at-a-time loop | Eight shift/XOR rounds per byte | Polynomial and on-disk format remain unchanged |
| Exact frame capacity hints | Payload encoders report realistic encoded size | Buffer growth/reallocation for TPC-C/YCSB rows | Hints must track encoding changes |
| Reserved-offset WAL option | Atomic tail reservation plus positioned writes | Shared producer queue/background writer | Unbatched syscalls lose; out-of-order durability must be tracked |
| Per-thread WAL batching | Combine local records before `pwrite` | One syscall per record | Flush boundaries and commit durability remain explicit |
| Lock-free table publication | `ArcSwap` table list; rare creation uses RCU replacement | Read-side database mutex/RwLock | Concurrent creation may rebuild on CAS retry and is intentionally cold-path |
| Runtime-sized worker structures | Drivers pass exact loader/OLTP/OLAP budget | Sizing every cache/log array to all machine CPUs | Budget must include every thread that can access the database |
| Fast workload RNG | Thread-local lightweight generators | Shared RNG locks and expensive generic distributions | Generated benchmark distribution must remain specification-equivalent |
| Pre-sized benchmark buffers | Known scan/row sizes reserve capacity | Driver reallocations that obscure engine cost | Does not change engine semantics or count as an index speedup |
| Native CPU code generation | Cargo config applies `target-cpu=native` | Generic x86 instruction selection | Cross-system comparisons require matching build policy |
| Payload-free existence query | `point_exists_si` returns hit/miss without cloning a result payload | Result vector and payload-handle construction when a benchmark explicitly requests key-only reads | Normal YCSB reads still load the payload by default; this path is opt-in |
| YCSB single-field patch | Generate one field patch and apply it while holding the leaf writer guard | Temporary full-row generation/copy for the standard update mode | Full-row replacement remains selectable for comparable experiments |
| Walker-alias Zipf sampling | Build the finite Zipf table once, then sample in O(1) | Floating-point power calculation per timed request | O(N) setup and additional memory; rank scrambling preserves realistic key placement |
| Scrambled Zipf ranks | Deterministically map popularity ranks across the key space | Artificial locality from placing hottest ranks on adjacent keys | Changes the generated workload, so old benchmark series are not directly comparable |
| Amortized benchmark clocks | Refresh time buckets periodically and sample scan latency | Clock reads and latency recording on every operation | Measurement resolution/sample count must be reported honestly |
| Disabled unbounded restart tracing | Expensive restart strings are off by default and reset between runs | Multi-gigabyte diagnostic accumulation and benchmark pollution | Enabling diagnostics is intentionally expensive |

## Reading the performance claims

- A bounded cost reduction—one fewer allocation, latch, payload load, retry, or copied
  row—is useful evidence but not a throughput percentage.
- Microbenchmarks isolate mechanisms; they do not include tree height, contention, WAL,
  GC, and workload skew unless explicitly stated.
- End-to-end benchmark numbers combine several effects and should not be assigned to one
  optimization independently.
- Page size always refers to `OptCell<Block>`, including `cell_version`; separately allocated
  payload bodies and the bitmap of an intentionally oversized leaf are outside that primary
  page allocation.

Additional focused notes are available for [range-scan iteration](range_scan_iteration.md),
[range-scan visibility](range_scan_visibility_check.md), and
[YCSB/HTAP behavior](ycsb_htap_optimizations.md).
