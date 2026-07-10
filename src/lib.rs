// use std::ffi::c_void;
// use std::{mem, ptr};
// use std::ops::Deref;
// use crate::mv_crud_model::crud_operation::CRUDOperation;
// use crate::mv_crud_model::crud_operation_result::CRUDOperationResult;
// use crate::mv_tree::mvbt::MVBTSt;
// use crate::mv_tx_model::transaction::AtomicTransaction;
// use mv_tx_query::tx_manager::TransactionManager;
// use crate::mv_root::index_root::RootIndexType;
// use crate::mv_utils::interval::Interval;
//
// mod mv_block;
// mod mv_crud_model;
// mod mv_page_model;
// mod mv_record_model;
// mod mv_tree;
// mod mv_utils;
// mod mv_test;
// mod mv_tx_model;
// mod mv_gc;
// mod mv_root;
// mod mv_query;
// mod mv_tx_query;
// mod mv_sync;
// mod mv_wal;
//
// const EX_FAN_OUT: usize = 127;
// const EX_N: usize = 127;
//
// type EX_KEY = u64;
// type EX_VALUE = u64;
//
// type MVBTreeApi = MVBTSt<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;
//
// pub const MONO: u8 = 0;
// pub const OLC: u8 = 2;
//
// pub const OPT_CLOCK: u8 = 0;
// pub const EXCL_CLOCK: u8 = 1;
// pub const FREE_CLOCK: u8 = 2;
// pub const GC_ENABLED: u8 = 1;
//
// struct MVBTreeWithGCApiExport(TransactionManager<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>);
//
// impl Deref for MVBTreeWithGCApiExport {
//     type Target = TransactionManager<EX_FAN_OUT, EX_N, EX_KEY, EX_VALUE>;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn init_tree_gc(protocol: u8, clock: u8, gc: u8) -> *mut c_void {
//     let index = match (protocol, clock) {
//         (OLC, OPT_CLOCK) => MVBTreeApi::make_standard(RootIndexType::default()),
//         (OLC, EXCL_CLOCK) => MVBTreeApi::make_standard(RootIndexType::default()),
//         _ => MVBTreeApi::default()
//     };
//
//     Box::into_raw(Box::new(MVBTreeWithGCApiExport(
//         TransactionManager::new_unmanaged(index, gc == GC_ENABLED)))) as _
// }
//
// /// Same as `init_tree_gc`, but backs the tree with a group-commit WAL at
// /// `wal_path` (UTF-8, `wal_path_len` bytes, not necessarily null-terminated):
// /// replays any existing log there before serving requests, then keeps
// /// appending to it, fsyncing every `flush_interval_ms`. Returns null on an
// /// invalid path or I/O error. `init_tree_gc` is left untouched so existing
// /// callers keep today's (WAL-less, in-memory-only) behavior.
// #[unsafe(no_mangle)]
// pub extern "C" fn init_tree_gc_wal(
//     _protocol: u8,
//     _clock: u8,
//     gc: u8,
//     wal_path: *const u8,
//     wal_path_len: usize,
//     flush_interval_ms: u64,
// ) -> *mut c_void {
//     let path_str = match std::str::from_utf8(unsafe {
//         std::slice::from_raw_parts(wal_path, wal_path_len)
//     }) {
//         Ok(s) => s,
//         Err(_) => return ptr::null_mut(),
//     };
//
//     let index = match MVBTreeApi::open_recovered(
//         RootIndexType::default(),
//         std::path::Path::new(path_str),
//         std::time::Duration::from_millis(flush_interval_ms),
//     ) {
//         Ok(tree) => tree,
//         Err(_) => return ptr::null_mut(),
//     };
//
//     Box::into_raw(Box::new(MVBTreeWithGCApiExport(
//         TransactionManager::new_unmanaged(index, gc == GC_ENABLED)))) as _
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn destroy_tree_gc_api(
//     api: *mut c_void)
// {
//     if !api.is_null() {
//         unsafe {
//             let _tree = Box::from_raw(api as *mut MVBTreeWithGCApiExport);
//         }
//     }
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn tree_gc_api_find(
//     api: *mut c_void,
//     key: *const u8,
//     sz: usize,
//     value_out: *mut u8) -> bool
// {
//     let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
//     api.find(key, sz, value_out)
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn tree_gc_api_insert(
//     api: *mut c_void,
//     key: *const u8,
//     key_sz: usize,
//     value: *const u8,
//     value_sz: usize) -> bool
// {
//     let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
//     api.insert(key, key_sz, value, value_sz)
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn tree_gc_api_update(
//     api: *mut c_void,
//     key: *const u8,
//     key_sz: usize,
//     value: *const u8,
//     value_sz: usize) -> bool
// {
//     let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
//     api.update(key, key_sz, value, value_sz)
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn tree_gc_api_remove(
//     api: *mut c_void,
//     key: *const u8,
//     key_sz: usize) -> bool
// {
//     let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
//     api.remove(key, key_sz)
// }
//
// #[unsafe(no_mangle)]
// pub extern "C" fn tree_gc_api_scan(
//     api: *mut c_void,
//     key: *const u8,
//     key_sz: usize,
//     scan_sz: i32,
//     values_out: *mut *mut u8) -> i32
// {
//     let api = unsafe { &*(api as *mut MVBTreeWithGCApiExport) };
//     api.scan(key, key_sz, scan_sz, values_out)
// }
//
// impl MVBTreeWithGCApiExport {
//     #[inline(always)]
//     fn find(&self, key: *const u8, _sz: usize, value_out: *mut u8) -> bool {
//         let querying_v
//             = self.index().current_version_for_reader();
//
//         match self.execute_on_caller_thread(AtomicTransaction::new(
//             Some(querying_v),
//             CRUDOperation::Point(unsafe { ptr::read(mem::transmute(key)) }, querying_v))
//         ).unwrap_atomic()
//         {
//             Ok((.., CRUDOperationResult::MatchedRecords(result)))
//             if !result.is_empty() => unsafe {
//                 ptr::write(mem::transmute(value_out), result.get_unchecked(0).payload);
//                 true
//             },
//             _ => false
//         }
//     }
//
//     #[inline(always)]
//     fn insert(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
//         match self.execute_on_caller_thread(AtomicTransaction::from_crud(CRUDOperation::Insert(
//             unsafe { ptr::read(mem::transmute(key)) },
//             unsafe { ptr::read(mem::transmute(value)) }))
//         ).unwrap_atomic()
//         {
//             Ok((.., CRUDOperationResult::Inserted(..))) => true,
//             _ => false
//         }
//     }
//
//     #[inline(always)]
//     fn update(&self, key: *const u8, _key_sz: usize, value: *const u8, _value_sz: usize) -> bool {
//         match self.execute_on_caller_thread(AtomicTransaction::from_crud(CRUDOperation::Update(
//             unsafe { ptr::read(mem::transmute(key)) },
//             unsafe { ptr::read(mem::transmute(value)) }))
//         ).unwrap_atomic()
//         {
//             Ok((.., CRUDOperationResult::Updated(..))) => true,
//             _ => false
//         }
//     }
//
//     #[inline(always)]
//     fn remove(&self, key: *const u8, _key_sz: usize) -> bool {
//         match self.execute_on_caller_thread(AtomicTransaction::from_crud(CRUDOperation::Delete(
//             unsafe { ptr::read(mem::transmute(key)) }))
//         ).unwrap_atomic()
//         {
//             Ok((.., CRUDOperationResult::Deleted(..))) => true,
//             _ => false
//         }
//     }
//
//     #[inline(always)]
//     fn scan(&self, key: *const u8, _key_sz: usize, mut scan_sz: i32, mut values_out: *mut *mut u8) -> i32 {
//         let querying_v
//             = self.index().current_version_for_reader();
//
//         let key_start = unsafe { *(key as *const u64) };
//         let key_end = key_start + scan_sz as u64 - 1;
//
//         match self.execute_on_caller_thread(AtomicTransaction::new(
//             Some(querying_v),
//             CRUDOperation::Range(Interval::new(key_start, key_end), querying_v))
//         ).unwrap_atomic()
//         {
//             Ok((.., CRUDOperationResult::MatchedRecords(mut buff))) => unsafe {
//                 buff.shrink_to_fit();
//
//                 let len = buff.len() as _;
//                 *values_out = buff.as_mut_ptr() as _;
//
//                 mem::forget(buff);
//                 len
//             }
//             _ => -1
//         }
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs;
//
//     #[test]
//     fn c_abi_wal_round_trip() {
//         let path = std::env::temp_dir().join(format!("cmvbt_lib_wal_test_{}.log", std::process::id()));
//         let _ = fs::remove_file(&path);
//         let path_str = path.to_str().unwrap();
//
//         let key: u64 = 42;
//         let value: u64 = 4242;
//
//         unsafe {
//             let api = init_tree_gc_wal(OLC, OPT_CLOCK, 0, path_str.as_ptr(), path_str.len(), 2);
//             assert!(!api.is_null());
//
//             assert!(tree_gc_api_insert(
//                 api,
//                 &key as *const u64 as *const u8, mem::size_of::<u64>(),
//                 &value as *const u64 as *const u8, mem::size_of::<u64>()));
//
//             let mut found: u64 = 0;
//             assert!(tree_gc_api_find(
//                 api, &key as *const u64 as *const u8, mem::size_of::<u64>(), &mut found as *mut u64 as *mut u8));
//             assert_eq!(found, value);
//
//             destroy_tree_gc_api(api);
//
//             // Reopen against the same log: the insert should be recovered.
//             let api2 = init_tree_gc_wal(OLC, OPT_CLOCK, 0, path_str.as_ptr(), path_str.len(), 2);
//             assert!(!api2.is_null());
//
//             let mut recovered_found: u64 = 0;
//             assert!(tree_gc_api_find(
//                 api2, &key as *const u64 as *const u8, mem::size_of::<u64>(), &mut recovered_found as *mut u64 as *mut u8));
//             assert_eq!(recovered_found, value);
//
//             destroy_tree_gc_api(api2);
//         }
//
//         let _ = fs::remove_file(&path);
//     }
// }