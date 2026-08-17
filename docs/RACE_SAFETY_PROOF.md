# Race Safety Proof: in_flight_bound Publication Window

## The Scenario (User's Question)

What if GC scans `in_flight_bound` and `live_tx` **before** the reader publishes its bound?

```
Timeline:
T₀: Reader reads current_version() = V₀ (not yet stored)
T₀: [GC scan happens, doesn't see V₀]
T₀: GC reclaims blocks based on live_min_snapshot = V_old
T₀: Reader stores V₀ to in_flight_bound[worker_id]
T₁: Clock advances: V₀ → V₅
T₂: Reader draws ts_start = V₅
T₃: Reader publishes live_tx[worker_id] = V₅
```

**Question:** Did GC reclaim a block that the reader at ts_start = V₅ needs?

## The Answer: NO - It's Safe

### Invariant: Reader Needs Blocks with death_version ≥ ts_start

A reader with `ts_start` needs a block only if:
```
death_version(block) ≥ ts_start
```

Why? Because a block with `death_version = D` is:
- **Alive at T** if `ts_start < D` (block dies AFTER reader's snapshot)
- **Dead at T** if `ts_start ≥ D` (block was already dead when reader started)

### GC's Reclamation Rule

GC reclaims blocks where:
```
death_version < live_min_snapshot_at_gc_time
```

This is safe because all active readers have `ts_start ≥ live_min_snapshot`, so they don't need those blocks.

### The Race Safety Proof

**Theorem:** A block reclaimed by GC in the past is never needed by a reader starting now.

**Proof:**

Let's denote:
- `T_past`: Time when GC ran (before reader published bound)
- `V_old`: `live_min_snapshot` at T_past
- `T_now`: Time when reader publishes `in_flight_bound[R]`
- `V₀`: `current_version()` at T_now (= `in_flight_bound[R]`)
- `V₁`: `ts_start` drawn after V₀ (= value in `live_tx[R]`)

**Clock Monotonicity:**
```
V_old < clock_at_T_past < V₀ < V₁
```

Therefore: `V₁ > V_old`

**For any block X that GC reclaimed:**
```
death_version(X) < V_old  (GC reclaimed it)
```

**For the reader to need block X:**
```
death_version(X) ≥ V₁  (reader's requirement)
```

**Contradiction:**
```
If death_version(X) < V_old and V_old < V₁
Then death_version(X) < V₁
Which contradicts death_version(X) ≥ V₁
```

Therefore, **any block GC reclaimed in the past cannot be needed by the reader starting now.** ✓

## Why This Works: Monotonic Clock + Ordering

The system relies on three properties:

### 1. **Clock is Strictly Monotonic**
```rust
let V_past = current_version();        // Some past time
let V_now = current_version();         // Now: V_now ≥ V_past
```

The global clock only ever increases. Future reads are >= past reads.

### 2. **Bound is Published Before ts_start is Drawn**
```rust
let bound = self.global_clock.current_version();     // V₀
self.in_flight_bound[worker_id].store(bound, Release); // Publish V₀
let ts_start = self.global_clock.next_timestamp();   // Draw V₁ ≥ V₀
```

Even if GC doesn't see the bound immediately, when ts_start is drawn, the clock has advanced.

### 3. **GC Protects Against Older Snapshots**
```rust
live_min_snapshot() includes:
  - All in_flight_bound values (being drawn right now)
  - All live_tx values (actively reading)
```

Once a reader publishes its bound, future GC scans see it. Past GC scans don't need to because `ts_start > past_live_min_snapshot` always.

## Visual Proof

```
Version Timeline:
V_old .... V₀ .... V₁ .... V₅
  ▲        ▲       ▲       ▲
  |        |       |       |
  |        |       |     ts_start drawn
  |        |     next_timestamp()
  |    in_flight_bound published
  |     (read clock here)
GC scan happened here
(doesn't see bound yet)

Reclaimed blocks: death_version < V_old
Reader needs: death_version ≥ V₁
Gap: V_old < V₁  ✓  (can't overlap)
```

## Why the Ordering Matters Despite the Window

The code looks like it should be vulnerable:

```rust
fn begin_snapshot_registration(&self) -> WorkerId {
    let worker_id = self.worker_id();
    let conservative_bound = self.global_clock.current_version();
    // ⚠️  GC could scan HERE (before store)
    self.in_flight_bound[worker_id as usize].store(conservative_bound, Release);
    worker_id
}
```

But it's not, because:

1. Even if GC scans before the `store`, it's scanning a **past** state
2. That past state determined `live_min_snapshot = V_old`
3. The reader will eventually draw `ts_start >= V₀ > V_old`
4. So the reader's future access pattern is unaffected by the past GC decision

The monotonic clock creates a **temporal barrier**: anything GC decided in the past is outdated by the time the reader starts, because new versions have been created.

## Conclusion

**There is no race condition.** The system is mathematically proven safe because:

✓ GC reclaims blocks with `death_version < V_old`  
✓ Reader draws `ts_start > V_old`  
✓ Therefore reader never needs reclaimed blocks  

The narrow window in the code is **provably safe** even though it exists.
