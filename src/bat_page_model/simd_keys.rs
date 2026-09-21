//! AVX2-accelerated scans over a leaf's `&[u64]` key region, with a plain
//! scalar fallback for non-`x86_64` targets or CPUs without AVX2 detected at
//! runtime (`is_x86_feature_detected!` is checked once per call — cheap, std
//! caches the CPUID probe internally after the first call).
//!
//! Only `u64` keys get the fast path: every real `Key` type in this codebase
//! (`bat_bench::tpcc_schema::TpccKey`, `bat_bench::ycsb_schema::YcsbKey`, and
//! the base tree's own default) is `u64`. `try_u64_keys`/`try_u64_scalar`
//! below use a `TypeId`-based check (stable Rust has no real specialization)
//! to reach this module from `LeafPage<N, Key, Payload>`'s generic call
//! sites without duplicating any of `LeafPage`'s own generic machinery, and
//! without narrowing `Key` to `u64` everywhere else. In today's codebase
//! this check is always `true` in practice, not a runtime cost that matters;
//! it just keeps this a strict addition rather than a constraint on `Key`.
//!
//! No AVX-512 path: the deployment target this was tuned for (AMD EPYC
//! 7742, Zen 2) doesn't implement AVX-512 at all — Zen 2 tops out at AVX2 —
//! so AVX2 already *is* "the best instruction set available" there, not a
//! conservative fallback pending a wider one. Revisit if a Zen 4+/Genoa
//! target is ever added.

use std::any::TypeId;

/// Reinterprets `&[Key]` as `&[u64]` when `Key` really is `u64`.
#[inline]
pub(crate) fn try_u64_keys<Key: 'static>(keys: &[Key]) -> Option<&[u64]> {
    if TypeId::of::<Key>() != TypeId::of::<u64>() {
        return None;
    }
    // SAFETY: just proved `Key` and `u64` are the same type, so `keys`'s
    // backing memory is already `len()` many `u64`s at this pointer.
    Some(unsafe { std::slice::from_raw_parts(keys.as_ptr() as *const u64, keys.len()) })
}

/// Reinterprets one `Key` value as `u64` when `Key` really is `u64` — the
/// scalar counterpart to `try_u64_keys`, for a lookup target or range bound.
#[inline]
pub(crate) fn try_u64_scalar<Key: 'static + Copy>(value: Key) -> Option<u64> {
    if TypeId::of::<Key>() != TypeId::of::<u64>() {
        return None;
    }
    // SAFETY: just proved `Key` and `u64` are the same type.
    Some(unsafe { *(&value as *const Key as *const u64) })
}

#[inline]
pub(crate) fn find_eq_desc(
    keys: &[u64],
    target: u64,
    mut on_candidate: impl FnMut(usize) -> bool,
) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: just checked AVX2 is available on this CPU.
            return unsafe { avx2::find_eq_desc_avx2(keys, target, &mut on_candidate) };
        }
    }
    find_eq_desc_scalar(keys, target, on_candidate)
}

pub(crate) fn find_eq_desc_scalar(
    keys: &[u64],
    target: u64,
    mut on_candidate: impl FnMut(usize) -> bool,
) -> bool {
    for i in (0..keys.len()).rev() {
        if keys[i] == target && on_candidate(i) {
            return true;
        }
    }
    false
}

#[allow(dead_code)]
#[inline]
pub(crate) fn for_each_in_range(
    keys: &[u64],
    lower: u64,
    upper: u64,
    mut on_match: impl FnMut(usize),
) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: just checked AVX2 is available on this CPU.
            unsafe { avx2::for_each_in_range_avx2(keys, lower, upper, &mut on_match) };
            return;
        }
    }
    for_each_in_range_scalar(keys, lower, upper, on_match)
}

pub(crate) fn for_each_in_range_scalar(
    keys: &[u64],
    lower: u64,
    upper: u64,
    mut on_match: impl FnMut(usize),
) {
    for (i, &k) in keys.iter().enumerate() {
        if k >= lower && k <= upper {
            on_match(i);
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod avx2 {
    use std::arch::x86_64::*;

    const LANES: usize = 4;

    /// # Safety
    /// Caller must have already confirmed `is_x86_feature_detected!("avx2")`.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn find_eq_desc_avx2(
        keys: &[u64],
        target: u64,
        on_candidate: &mut dyn FnMut(usize) -> bool,
    ) -> bool {
        let n = keys.len();
        let full_chunks = n / LANES;
        let tail = n % LANES;
        let target_v = unsafe { _mm256_set1_epi64x(target as i64) };

        for chunk in (0..full_chunks).rev() {
            let base = tail + chunk * LANES;
            let mask = unsafe {
                let v = _mm256_loadu_si256(keys.as_ptr().add(base) as *const __m256i);
                let eq = _mm256_cmpeq_epi64(v, target_v);
                _mm256_movemask_pd(_mm256_castsi256_pd(eq))
            };
            if mask != 0 {
                for lane in (0..LANES).rev() {
                    if mask & (1 << lane) != 0 && on_candidate(base + lane) {
                        return true;
                    }
                }
            }
        }

        for i in (0..tail).rev() {
            if keys[i] == target && on_candidate(i) {
                return true;
            }
        }

        false
    }

    /// # Safety
    /// Caller must have already confirmed `is_x86_feature_detected!("avx2")`.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn for_each_in_range_avx2(
        keys: &[u64],
        lower: u64,
        upper: u64,
        on_match: &mut dyn FnMut(usize),
    ) {
        let n = keys.len();
        let full_chunks = n / LANES;
        let tail_start = full_chunks * LANES;

        let sign = unsafe { _mm256_set1_epi64x(i64::MIN) };
        let lower_v = unsafe { _mm256_xor_si256(_mm256_set1_epi64x(lower as i64), sign) };
        let upper_v = unsafe { _mm256_xor_si256(_mm256_set1_epi64x(upper as i64), sign) };
        let all_ones = unsafe { _mm256_set1_epi64x(-1) };

        for chunk in 0..full_chunks {
            let base = chunk * LANES;
            let mask = unsafe {
                let v = _mm256_loadu_si256(keys.as_ptr().add(base) as *const __m256i);
                let biased = _mm256_xor_si256(v, sign);
                // out of range  <=>  biased < lower_v  OR  biased > upper_v
                let below_lower = _mm256_cmpgt_epi64(lower_v, biased);
                let above_upper = _mm256_cmpgt_epi64(biased, upper_v);
                let out_of_range = _mm256_or_si256(below_lower, above_upper);
                let in_range = _mm256_xor_si256(out_of_range, all_ones);
                _mm256_movemask_pd(_mm256_castsi256_pd(in_range))
            };
            if mask != 0 {
                for lane in 0..LANES {
                    if mask & (1 << lane) != 0 {
                        on_match(base + lane);
                    }
                }
            }
        }

        for i in tail_start..n {
            if keys[i] >= lower && keys[i] <= upper {
                on_match(i);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Small xorshift PRNG so this test doesn't need an external crate —
    // deterministic across runs given the same seed, which is all a
    // differential test against a scalar reference needs.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    fn avx2_available() -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    }

    fn collect_eq_desc(
        f: impl Fn(&[u64], u64, &mut dyn FnMut(usize) -> bool) -> bool,
        keys: &[u64],
        target: u64,
    ) -> Vec<usize> {
        let mut found = Vec::new();
        f(keys, target, &mut |i| {
            found.push(i);
            false // never stop early — we want every candidate, to compare full sets
        });
        found
    }

    fn collect_range(
        f: impl Fn(&[u64], u64, u64, &mut dyn FnMut(usize)),
        keys: &[u64],
        lower: u64,
        upper: u64,
    ) -> Vec<usize> {
        let mut found = Vec::new();
        f(keys, lower, upper, &mut |i| found.push(i));
        found
    }

    #[test]
    fn eq_desc_avx2_matches_scalar_reference_across_random_inputs() {
        if !avx2_available() {
            eprintln!(
                "AVX2 not available on this CPU — skipping (scalar-only path is exercised elsewhere)"
            );
            return;
        }
        let mut rng = Rng(0x9E3779B97F4A7C15);
        let lens = [
            0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 123, 200, 1019, 4001,
        ];
        for &len in &lens {
            for trial in 0..8 {
                let universe: u64 = if trial % 2 == 0 { 8 } else { u64::MAX };
                let keys: Vec<u64> = (0..len).map(|_| rng.next() % universe.max(1)).collect();
                let targets: [u64; 4] = [
                    0,
                    universe.saturating_sub(1),
                    rng.next() % universe.max(1),
                    u64::MAX,
                ];
                for &target in &targets {
                    let expected = collect_eq_desc(
                        |k, t, cb| find_eq_desc_scalar(k, t, |i| cb(i)),
                        &keys,
                        target,
                    );
                    let actual = collect_eq_desc(
                        |k, t, cb| unsafe { avx2::find_eq_desc_avx2(k, t, cb) },
                        &keys,
                        target,
                    );
                    assert_eq!(
                        actual, expected,
                        "len={len} trial={trial} universe={universe} target={target} keys={keys:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn eq_desc_avx2_stops_at_the_first_candidate_that_returns_true() {
        if !avx2_available() {
            return;
        }
        let keys = vec![5u64, 3, 5, 5, 1, 5, 2];
        // Reverse order means index 5 (value 5) should be the first
        // candidate visited, then index 3, then index 2, then index 0.
        let mut visited = Vec::new();
        let stopped = unsafe {
            avx2::find_eq_desc_avx2(&keys, 5, &mut |i| {
                visited.push(i);
                visited.len() == 2 // stop right after the second candidate
            })
        };
        assert!(stopped);
        assert_eq!(visited, vec![5, 3]);
    }

    #[test]
    fn range_avx2_matches_scalar_reference_across_random_inputs() {
        if !avx2_available() {
            eprintln!(
                "AVX2 not available on this CPU — skipping (scalar-only path is exercised elsewhere)"
            );
            return;
        }
        let mut rng = Rng(0xD1B54A32D192ED03);
        let lens = [
            0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 123, 200, 1019, 4001,
        ];
        for &len in &lens {
            for trial in 0..8 {
                let universe: u64 = if trial % 2 == 0 { 32 } else { u64::MAX };
                let keys: Vec<u64> = (0..len).map(|_| rng.next() % universe.max(1)).collect();
                let bounds = [
                    (0u64, universe.max(1) - 1),            // full range for that universe
                    (u64::MIN, u64::MAX), // truly unrestricted (full-table scan shape)
                    (universe / 2, universe / 2), // single-value range
                    (universe.max(2), universe.max(2) - 1), // inverted -> always empty
                ];
                for &(lower, upper) in &bounds {
                    let expected = collect_range(
                        |k, lo, hi, cb| for_each_in_range_scalar(k, lo, hi, |i| cb(i)),
                        &keys,
                        lower,
                        upper,
                    );
                    let actual = collect_range(
                        |k, lo, hi, cb| unsafe { avx2::for_each_in_range_avx2(k, lo, hi, cb) },
                        &keys,
                        lower,
                        upper,
                    );
                    assert_eq!(
                        actual, expected,
                        "len={len} trial={trial} universe={universe} lower={lower} upper={upper} keys={keys:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn try_u64_keys_and_scalar_only_engage_for_the_u64_type() {
        let keys: [u64; 3] = [1, 2, 3];
        assert_eq!(try_u64_keys(&keys), Some(&keys[..]));
        assert_eq!(try_u64_scalar::<u64>(42), Some(42));

        let not_u64: [i32; 3] = [1, 2, 3];
        assert_eq!(try_u64_keys(&not_u64), None);
        assert_eq!(try_u64_scalar::<i32>(42), None);
    }
}
