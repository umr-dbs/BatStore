use crate::bat_block::block::BlockGuard;
use crate::bat_crud_model::crud_operation_result::CRUDOperationResult;
use crate::bat_page_model::leaf_page::{LeafPage, LeafRecordRef};
use crate::bat_page_model::node::PageType;
use crate::bat_page_model::time_matcher::TimeMatcher;
use crate::bat_page_model::{Attempts, BlockRef};
use crate::bat_query::interval::Interval;
use crate::bat_record_model::record_point::RecordPointResult;
use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::Version;
use crate::bat_root::index_root::RootIndex;
use crate::bat_sync::smart_cell::sched_yield;
use crate::bat_tree::mvbt::MVBTSt;
use itertools::Itertools;
use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    pub fn retrieve_root_number_for(&self, lookup_version: Version) -> (usize, usize) {
        match self.root {
            RootIndex::FrugalList(ref fg) => {
                let roots = fg;
                let roots_all = roots.iter().collect_vec();

                let root_count = roots_all.len();

                (
                    roots_all
                        .iter()
                        .enumerate()
                        .rev()
                        .find_map(|(pos, r)| (r.insert_version <= lookup_version).then(|| pos + 1))
                        .unwrap(),
                    root_count,
                )
            }
            RootIndex::LinkedList(ref ll) => {
                let roots = ll.clone();
                let root_count = roots.len();

                (
                    roots
                        .iter()
                        .enumerate()
                        .rev()
                        .find_map(|(pos, r)| {
                            (r.version <= lookup_version).then(|| pos + 1).or(Some(1))
                        })
                        .unwrap(),
                    root_count,
                )
            }
            _ => (0, 0),
        }
    }

    #[inline(always)]
    pub fn retrieve_root_for(
        &self,
        lookup_version: Version,
    ) -> BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload> {
        self.root.root_for(lookup_version).block
    }

    #[inline]
    fn traverse_read_key(
        root: &BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        lookup_version: Version,
    ) -> BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload> {
        loop {
            let mut curr = root.borrow_read();

            while let PageType::IndexRef(internal_page) = curr.as_page_ref() {
                let (keys_page, versions_page) = internal_page.keys_versions();

                curr = versions_page
                    .iter()
                    .zip(keys_page)
                    .enumerate()
                    .rfind(|(_, (v, range))| v.matched(lookup_version) && range.contains(key))
                    .map(|(pos, _)| internal_page.get_pointer(pos).borrow_read())
                    .unwrap();
            }

            break curr;
        }
    }

    fn traverse_read_key_range(
        mut curr: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: &Interval<Key>,
        lookup_version: Version,
    ) -> Vec<BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>> {
        let mut blocks = VecDeque::new();

        blocks.push_back(curr);

        let mut leafs = vec![];

        while !blocks.is_empty() {
            curr = blocks.pop_front().unwrap();

            match curr.as_page_ref() {
                PageType::IndexRef(internal_page) => {
                    let (keys_page, versions_page) = internal_page.keys_versions();

                    let start_pos_si = versions_page.len()
                        - versions_page
                            .binary_search_by(|v| v.into_cmp().cmp(&lookup_version))
                            .unwrap_or_else(|pos| pos);

                    versions_page
                        .iter()
                        .enumerate()
                        .zip(keys_page.iter())
                        .rev()
                        .skip(start_pos_si)
                        .filter(|((.., v), range)| //v.matched(lookup_version) &&
                            v.matched(lookup_version) && lookup_range.overlap(range))
                        .unique_by(|(.., range)| range.lower())
                        .unique_by(|(.., range)| range.upper())
                        .for_each(|((pos, ..), ..)| {
                            blocks.push_back(internal_page.get_pointer(pos).clone())
                        });
                }
                _ => leafs.push(curr),
            }
        }

        leafs
    }

    #[inline]
    pub(crate) fn scan_leaf_for_key<'a, F: FnMut(TxStamp) -> bool>(
        leaf: &'a LeafPage<NUM_RECORDS, Key, Payload>,
        key: Key,
        is_visible: &mut F,
    ) -> Option<LeafRecordRef<'a, Key, Payload>> {
        leaf.keys()
            .iter()
            .enumerate()
            .rev()
            .filter(|(_, k)| **k == key)
            .map(|(i, _)| leaf.record(i))
            .find(|r| r.version().matches(is_visible))
    }

    #[inline]
    pub(crate) fn key_point_read_from_root<'a>(
        &self,
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        key: Key,
        reader_worker: WorkerId,
        reader_ts_start: Version,
    ) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let records = Self::traverse_read_key(&root, key, reader_ts_start);

        let leaf = records.as_leaf_page_ref();
        let keys = leaf.keys();

        self.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| crate::bat_sync::visibility::is_visible(
                commit_logs, cache, reader_worker, reader_ts_start, stamp);

            let mut found = None;
            let mut check_candidate = |i: usize| {
                let r = leaf.record(i);
                if r.version().matches(&mut is_visible) {
                    found = Some(r);
                    true
                } else {
                    false
                }
            };

            match crate::bat_page_model::simd_keys::try_u64_scalar(key)
                .zip(crate::bat_page_model::simd_keys::try_u64_keys(keys))
            {
                Some((target, u64_keys)) => {
                    crate::bat_page_model::simd_keys::find_eq_desc(u64_keys, target, &mut check_candidate);
                }
                None => {
                    keys.iter().enumerate().rev()
                        .filter(|(_, k)| **k == key)
                        .any(|(i, _)| check_candidate(i));
                }
            }

            match found {
                None => {
                    if crate::bat_test::DIAG {
                        let same_key: Vec<String> = keys.iter().enumerate()
                            .filter(|(_, k)| **k == key)
                            .map(|(i, _)| {
                                let r = leaf.record(i);
                                format!(
                                    "insert=(w{},{}) invalid={} deleted={} is_vis_insert={} is_vis_del={:?}",
                                    r.version.insertion_stamp().worker_id(),
                                    r.version.insertion_stamp().ts_start(),
                                    r.version.insertion_stamp().is_invalid(),
                                    r.version.deletion_stamp().map(|d| d.to_string()).unwrap_or_else(|| "*".to_string()),
                                    is_visible(r.version.insertion_stamp()),
                                    r.version.deletion_stamp().map(|d| is_visible(d)),
                                )
                            }).collect();
                        eprintln!(
                            "DIAG key_point_read_from_root: MISS key={key} reader=(w{reader_worker},{reader_ts_start}) leaf_len={} same_key_records={same_key:?}",
                            keys.len(),
                        );
                    }
                    CRUDOperationResult::MatchedRecords(Vec::with_capacity(0))
                }
                Some(result) =>
                    CRUDOperationResult::MatchedRecords(vec![RecordPointResult::from_leaf(result)])
            }
        })
    }

    #[inline]
    pub fn point_exists_si(&self, key: Key) -> bool {
        let reader_worker = self.worker_id();
        let reader_ts_start = self.begin_snapshot();
        let root = self.retrieve_root_for(reader_ts_start);
        let records = Self::traverse_read_key(&root, key, reader_ts_start);
        let found = self.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| {
                crate::bat_sync::visibility::is_visible(
                    commit_logs,
                    cache,
                    reader_worker,
                    reader_ts_start,
                    stamp,
                )
            };
            let leaf = records.as_leaf_page_ref();
            Self::scan_leaf_for_key(leaf, key, &mut is_visible).is_some()
        });
        self.end_snapshot(reader_ts_start);
        found
    }

    pub(crate) fn key_range_read_from_root<'a>(
        &self,
        root: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        lookup_range: Interval<Key>,
        reader_worker: WorkerId,
        reader_ts_start: Version,
    ) -> CRUDOperationResult<'a, FAN_OUT, NUM_RECORDS, Key, Payload> {
        let blocks = Self::traverse_read_key_range(root, &lookup_range, reader_ts_start);

        self.with_snapshot_cache_and_logs(|cache, commit_logs| {
            let mut is_visible = |stamp| {
                crate::bat_sync::visibility::is_visible(
                    commit_logs,
                    cache,
                    reader_worker,
                    reader_ts_start,
                    stamp,
                )
            };
            CRUDOperationResult::MatchedRecords(
                blocks
                    .into_iter()
                    .map(|leaf| {
                        let records = leaf.deref().as_records();

                        records
                            .iter()
                            .rev()
                            // Cheap range check before the indirect (`&mut dyn
                            // FnMut`, not inlinable) `matches` call — see
                            // `iter_query.rs::refill`'s identical reorder for why.
                            .filter(|r| {
                                lookup_range.contains(r.key())
                                    && r.version().matches(&mut is_visible)
                            })
                            // .sorted_by_key(|r| r.key())
                            .map(RecordPointResult::from_leaf)
                            .collect::<Vec<_>>()
                    })
                    // .filter(|set| !set.is_empty())
                    // .sorted_by_key(|set|
                    //     unsafe { set.get_unchecked(0).key })
                    .flatten()
                    .collect(),
            )
        })
    }

}
