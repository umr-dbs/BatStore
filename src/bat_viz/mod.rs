//! Structural dump of an `MVBTSt` (all root* versions + the physical block
//! graph they point into) to a JSON file the standalone
//! `tools/tree_visualizer.html` page can load and render. Debug/inspection
//! tooling only - not on any path the benchmarks or the paper depend on,
//! hence gated behind the `tree-viz` feature (see `dump`'s module doc for
//! the read-safety caveat this implies).

pub mod dump;
