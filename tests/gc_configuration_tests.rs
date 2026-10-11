use super::{GcShardLayout, PageReclaimMode, parse_cli_gc_settings};

#[test]
fn reclaim_modes_have_stable_configuration_names() {
    for (name, mode) in [
        ("off", PageReclaimMode::Off),
        ("reuse-no-steal", PageReclaimMode::ReuseNoSteal),
        ("reuse-steal", PageReclaimMode::ReuseSteal),
        ("os", PageReclaimMode::Os),
    ] {
        assert_eq!(name.parse::<PageReclaimMode>().unwrap(), mode);
        assert_eq!(mode.to_string(), name);
    }
    assert!("invalid".parse::<PageReclaimMode>().is_err());
}

#[test]
fn shard_layout_resolves_against_worker_capacity() {
    assert_eq!(GcShardLayout::PerWorker.shard_count(17), 17);
    assert_eq!(GcShardLayout::fixed(3).shard_count(17), 3);
    assert_eq!("per-worker".parse(), Ok(GcShardLayout::PerWorker));
    assert_eq!("3".parse(), Ok(GcShardLayout::fixed(3)));
    assert_eq!("fixed:3".parse(), Ok(GcShardLayout::fixed(3)));
    assert_eq!(GcShardLayout::fixed(3).to_string(), "3");
    assert!("0".parse::<GcShardLayout>().is_err());
    assert!("invalid".parse::<GcShardLayout>().is_err());
}

#[test]
#[should_panic(expected = "requires at least one shard")]
fn fixed_shard_layout_rejects_zero() {
    let _ = GcShardLayout::fixed(0);
}

#[test]
fn legacy_cli_gc_arguments_keep_their_defaults() {
    assert_eq!(
        parse_cli_gc_settings(true, None, None),
        Ok((PageReclaimMode::ReuseSteal, GcShardLayout::PerWorker))
    );
    assert_eq!(
        parse_cli_gc_settings(false, None, None),
        Ok((PageReclaimMode::Off, GcShardLayout::PerWorker))
    );
}

#[test]
fn cli_gc_arguments_accept_explicit_mode_and_shard_count() {
    assert_eq!(
        parse_cli_gc_settings(true, Some("reuse-no-steal"), Some("3")),
        Ok((PageReclaimMode::ReuseNoSteal, GcShardLayout::fixed(3)))
    );
    assert_eq!(
        parse_cli_gc_settings(true, Some("os"), Some("per-worker")),
        Ok((PageReclaimMode::Os, GcShardLayout::PerWorker))
    );
    assert!(parse_cli_gc_settings(true, Some("unknown"), None).is_err());
    assert!(parse_cli_gc_settings(true, None, Some("0")).is_err());
}

#[test]
fn legacy_disabled_gc_cannot_be_reenabled_by_the_new_mode_argument() {
    assert_eq!(
        parse_cli_gc_settings(false, Some("reuse-steal"), Some("2")),
        Ok((PageReclaimMode::Off, GcShardLayout::fixed(2)))
    );
}
