use crate::bat_root::root::Root;
use std::collections::LinkedList;

pub(crate) type VanillaRootSt<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    LinkedList<Root<FAN_OUT, NUM_RECORDS, Key, Payload>>;
