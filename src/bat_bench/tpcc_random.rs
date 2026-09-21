//! TPC-C's specified non-uniform random distributions (spec §2.1.6) and
//! string generators (§4.3.2.2), reimplemented for benchmark purposes (not
//! an audited kit: the `C` run-constants below are fixed rather than chosen
//! per the spec's randomized-once-per-run procedure, which does not affect
//! the shape of the workload).

use std::cell::RefCell;

thread_local! {
    static FAST_RNG: RefCell<fastrand::Rng> = RefCell::new(fastrand::Rng::new());
}

#[inline]
pub fn with_fast_rng<R>(f: impl FnOnce(&mut fastrand::Rng) -> R) -> R {
    FAST_RNG.with(|rng| f(&mut rng.borrow_mut()))
}

/// NURand(A, x, y) = (((random(0,A) | random(x,y)) + C) % (y-x+1)) + x
#[inline]
pub fn nurand(a: u64, x: u64, y: u64, c: u64) -> u64 {
    let (r1, r2) = with_fast_rng(|rng| (rng.u64(0..=a), rng.u64(x..=y)));
    (((r1 | r2) + c) % (y - x + 1)) + x
}

const C_CUSTOMER_ID: u64 = 259;
const C_ITEM_ID: u64 = 7_911;
const C_LAST_LOAD: u64 = 157;
const C_LAST_RUN: u64 = 223;

/// Customer id within a district, per NURand(1023, 1, num_customers).
#[inline]
pub fn nu_rand_customer_id(num_customers: u32) -> u32 {
    nurand(1023, 1, num_customers as u64, C_CUSTOMER_ID) as u32
}

/// Item id, per NURand(8191, 1, num_items).
#[inline]
pub fn nu_rand_item_id(num_items: u32) -> u32 {
    nurand(8191, 1, num_items as u64, C_ITEM_ID) as u32
}

/// C_LAST syllable code (0..=999) used at *load* time: the first 1,000
/// customers of a district get codes 0..=999 directly (so every syllable
/// combination has >= 1 customer up front); the rest draw NURand(255,0,999).
#[inline]
pub fn c_last_code_for_load(customer_ordinal_zero_based: u32) -> u16 {
    if customer_ordinal_zero_based < 1000 {
        customer_ordinal_zero_based as u16
    } else {
        nurand(255, 0, 999, C_LAST_LOAD) as u16
    }
}

/// C_LAST syllable code used at *run* time by Payment/OrderStatus's
/// "look up by last name" path: NURand(255, 0, 999).
#[inline]
pub fn c_last_code_for_run() -> u16 {
    nurand(255, 0, 999, C_LAST_RUN) as u16
}

const SYLLABLES: [&str; 10] = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

/// Deterministically expands a 0..=999 code into the TPC-C C_LAST name
/// (3 syllables chosen by the code's hundreds/tens/units digits).
pub fn gen_last_name(code: u16) -> String {
    let code = code as usize;
    format!(
        "{}{}{}",
        SYLLABLES[code / 100],
        SYLLABLES[(code / 10) % 10],
        SYLLABLES[code % 10]
    )
}

/// Order-preserving-ish 16-bit surrogate for a first name, used only to
/// break ties among same-last-name customers by (approximate) first-name
/// order — TPC-C's "pick the (n+1)/2-th customer ordered by c_first".
pub fn first_code(s: &str) -> u16 {
    let rank = |b: u8| -> u64 {
        match b {
            b'0'..=b'9' => (b - b'0') as u64,
            b'A'..=b'Z' => 10 + (b - b'A') as u64,
            b'a'..=b'z' => 10 + (b - b'a') as u64,
            _ => 0,
        }
    };
    let bytes = s.as_bytes();
    let get = |i: usize| bytes.get(i).copied().unwrap_or(b'0');
    let packed = rank(get(0)) * 36 * 36 + rank(get(1)) * 36 + rank(get(2));
    (packed % (u16::MAX as u64 + 1)) as u16
}

/// TPC-C a-string: random length in `[min, max]` of alphanumeric characters.
pub fn rnd_astring(min: usize, max: usize) -> String {
    with_fast_rng(|rng| {
        let len = rng.usize(min..=max);
        (0..len).map(|_| rng.alphanumeric()).collect()
    })
}

pub fn rnd_astring_exact<const N: usize>() -> [u8; N] {
    with_fast_rng(|rng| std::array::from_fn(|_| rng.alphanumeric() as u8))
}

/// TPC-C n-string: random length in `[min, max]` of decimal digits.
pub fn rnd_nstring(min: usize, max: usize) -> String {
    with_fast_rng(|rng| {
        let len = rng.usize(min..=max);
        (0..len).map(|_| rng.digit(10)).collect()
    })
}

/// TPC-C "original data": an a-string in `[min, max]`, with a 1-in-10 chance
/// of embedding the literal substring "ORIGINAL" (used by i_data/s_data;
/// NewOrder's brand-generic marker checks for this substring).
pub fn rnd_original_data(min: usize, max: usize) -> String {
    let mut s = rnd_astring(min, max);
    if with_fast_rng(|rng| rng.u32(0..10)) == 0 && s.len() >= 8 {
        let pos = with_fast_rng(|rng| rng.usize(0..=(s.len() - 8)));
        s.replace_range(pos..pos + 8, "ORIGINAL");
    }
    s
}

pub fn rnd_zip() -> String {
    format!("{}11111", rnd_nstring(4, 4))
}

pub fn rnd_phone() -> String {
    rnd_nstring(16, 16)
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
