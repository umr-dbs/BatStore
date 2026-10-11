pub mod block_tracer;
pub mod tracker_handle;

use std::num::NonZeroUsize;

/// Construction-time layout of the physical page-reclamation queues.
///
/// A fixed layout deliberately permits several workers to share one shard.
/// In that layout `ReuseNoSteal` means "stay within the worker's assigned
/// shard", so workers mapped to the same shard may reuse each other's pages.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GcShardLayout {
    /// Allocate one shard per registered transactional worker.
    #[default]
    PerWorker,
    /// Allocate exactly this many shards and map workers by modulo.
    Fixed(NonZeroUsize),
}

impl GcShardLayout {
    /// Creates a fixed shard layout, panicking when `shard_count` is zero.
    pub fn fixed(shard_count: usize) -> Self {
        Self::Fixed(
            NonZeroUsize::new(shard_count)
                .expect("GcShardLayout::fixed requires at least one shard"),
        )
    }

    #[inline(always)]
    pub(crate) fn shard_count(self, max_workers: usize) -> usize {
        match self {
            Self::PerWorker => max_workers.max(1),
            Self::Fixed(count) => count.get(),
        }
    }
}

impl std::fmt::Display for GcShardLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PerWorker => f.write_str("per-worker"),
            Self::Fixed(count) => count.fmt(f),
        }
    }
}

impl std::str::FromStr for GcShardLayout {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim().to_ascii_lowercase();
        if matches!(value.as_str(), "per-worker" | "per_worker" | "workers") {
            return Ok(Self::PerWorker);
        }
        let count = value
            .strip_prefix("fixed:")
            .unwrap_or(&value)
            .parse::<usize>()
            .map_err(|_| {
                format!("invalid GC shard layout '{value}'; expected per-worker or a positive integer")
            })?;
        NonZeroUsize::new(count).map(Self::Fixed).ok_or_else(|| {
            "invalid GC shard layout '0'; shard count must be positive".to_string()
        })
    }
}

/// Physical policy for pages retired by MVCC/structural modifications.
///
/// This is intentionally separate from record compaction and from
/// `update_in_place`: it controls only where a future page allocation comes
/// from and what happens to pages once snapshot reclamation says they are no
/// longer reachable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum PageReclaimMode {
    /// Never queue or reuse retired pages. Allocations are always fresh.
    #[default]
    Off = 0,
    /// Reuse eligible pages from the requesting worker's assigned shard only.
    ReuseNoSteal = 1,
    /// Reuse the assigned shard first, then eligible pages from other shards.
    ReuseSteal = 2,
    /// Destroy eligible retired pages and satisfy misses from fresh batches.
    Os = 3,
}

impl std::fmt::Display for PageReclaimMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Off => "off",
            Self::ReuseNoSteal => "reuse-no-steal",
            Self::ReuseSteal => "reuse-steal",
            Self::Os => "os",
        })
    }
}

impl std::str::FromStr for PageReclaimMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "reuse-no-steal" | "reuse_no_steal" | "nosteal" => Ok(Self::ReuseNoSteal),
            "reuse-steal" | "reuse_steal" | "steal" => Ok(Self::ReuseSteal),
            "os" => Ok(Self::Os),
            _ => Err(format!(
                "unknown page reclaim mode '{value}'; expected off, reuse-no-steal, reuse-steal, or os"
            )),
        }
    }
}

impl PageReclaimMode {
    #[inline(always)]
    pub(crate) const fn enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

/// Resolves the optional CLI additions without changing the legacy `gc`
/// Boolean's behavior. The mode and shard layout are deliberately trailing
/// arguments in every benchmark command, so old invocations continue to use
/// `ReuseSteal` with one shard per worker.
pub(crate) fn parse_cli_gc_settings(
    enabled: bool,
    mode: Option<&str>,
    shards: Option<&str>,
) -> Result<(PageReclaimMode, GcShardLayout), String> {
    let mode = if enabled {
        mode.map(str::parse)
            .transpose()?
            .unwrap_or(PageReclaimMode::ReuseSteal)
    } else {
        PageReclaimMode::Off
    };
    let shards = shards.map(str::parse).transpose()?.unwrap_or_default();
    Ok((mode, shards))
}

#[cfg(test)]
#[path = "../../tests/gc_configuration_tests.rs"]
mod tests;
