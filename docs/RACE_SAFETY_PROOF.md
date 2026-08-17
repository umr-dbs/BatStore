# Race Safety Analysis: in_flight_bound Publication Window

## The Critical Question (User's Challenge)

What if GC scans `in_flight_bound` and `live_tx` **before** the reader's Release store completes?

```
Timeline (User's Scenario):
T₀: Reader loads current_version() = V₀ (into register, NOT yet stored)
T₀: [Clock advances rapidly: V₀ → V₅ → V₁₀ (other threads making progress)]
T₀: GC scans in_flight_bound[reader] → sees NOT_IN_FLIGHT (store not visible!)
T₀: GC computes live_min_snapshot from other active readers = V_old
T₀: GC reclaims all blocks with death_version < V_old
T₁: Reader finally executes store: in_flight_bound[reader] = V₀ (Release)
T₂: Reader draws ts_start = next_timestamp() = V₁₀
T₃: Reader publishes live_tx[reader] = V₁₀
```

**Question:** Can reader with `ts_start = V₁₀` need a block that GC already reclaimed?

## The Critical Invariant: Reader Needs Blocks with death_version > ts_start

A reader with `ts_start = S` needs a block only if:
```
death_version(block) > ts_start
```

Why? A block with `death_version = D`:
- Is **alive at time S** if `D > S` (block dies AFTER reader's snapshot)
- Is **dead at time S** if `D ≤ S` (block was already dead when reader started)

GC reclaims blocks where:
```
death_version < live_min_snapshot_at_gc_time
```

## The Potential Race Condition (User's Valid Concern)

**For a correctness failure to occur:**
- Block X has `death_version = V` 
- `V < V_old` (GC reclaimed it: `death_version < live_min_snapshot`)
- `V > ts_start` (Reader needs it: `death_version > ts_start`)

**This requires:** `ts_start < V < V_old`, which means `ts_start < V_old`

### The Key Question: Can `ts_start < V_old`?

`V_old = live_min_snapshot` at GC scan time = minimum of ALL active snapshots at that moment.

**Example of the concern:**
- Long-running reader R_old has `ts_start = 100` (still active at GC scan time T₀)
- `V_old ≤ 100`
- New reader arrives later and draws `ts_start_new`
- **Can `ts_start_new < 100`?** ← This is the critical question

## The Answer: NO - Monotonic Clock Prevents It

**Why `ts_start_new ≥ V_old` always:**

Let's trace the timeline carefully:

1. **Old reader R_old** drew `ts_start_old = 100` at some time T_old ≤ T₀
   - The clock value was ~100 at that time
   
2. **At GC scan time T₀:**
   - Current clock ≥ 100 (monotonic: clock never decreases)
   - `live_min_snapshot = 100` (includes R_old)
   
3. **New reader R_new arrives at time T_now ≥ T₀:**
   - Draws `ts_start_new` from `next_timestamp()`
   - Current clock at T_now ≥ current clock at T₀ ≥ 100
   - Therefore `ts_start_new ≥ 100 ≥ V_old`

**Therefore:** `ts_start_new ≥ V_old` always

### Consequence

For blocks reclaimed by GC: `death_version < V_old`
For blocks reader needs: `death_version > ts_start_new ≥ V_old`

These ranges don't overlap! ✓

```
Versions reclaimed:   death_version < V_old
                      [0 ..................... V_old)

Reader's needs:       death_version > ts_start_new ≥ V_old
                                              V_old ... ∞)

No overlap! ✓
```

## The Role of Release/Acquire Synchronization

The Release/Acquire pair (`store(Release)` / `load(Acquire)`) serves two purposes:

1. **For GC scans BEFORE the store:** Safe due to monotonic clock (proven above)
2. **For GC scans AFTER the store:** See the bound immediately (tighter protection)

```rust
fn begin_snapshot_registration(&self) -> WorkerId {
    let conservative_bound = self.global_clock.current_version();
    self.in_flight_bound[worker_id].store(conservative_bound, Release);
    //↑ Release ensures visibility to subsequent Acquire loads
}

fn live_min_snapshot(&self) -> Option<SnapShot> {
    for slot in &self.in_flight_bound {
        let bound = slot.load(Acquire);  // Pairs with Release above
        // ...
    }
}
```

The Release/Acquire is **not the primary safety mechanism**. It's an **optimization** that ensures future GC scans see the bound immediately, rather than relying only on clock monotonicity.

## Conclusion: The System Is Race-Safe

**The guarantee holds due to monotonic clock, not just synchronization:**

✓ GC reclaims blocks with `death_version < V_old` at GC scan time  
✓ Any new reader arriving after that GC scan will draw `ts_start ≥ V_old` (monotonic clock)  
✓ New reader only needs blocks with `death_version > ts_start`  
✓ Therefore no overlap: GC-reclaimed blocks are never needed  

**Release/Acquire provides:**
- Immediate visibility for subsequent GC scans (tighter bound)
- Stronger formal guarantees
- But is not the sole source of safety—the clock itself is the primary barrier

The system is safe even though the window exists: **the clock's monotonicity is the true safety mechanism**.
