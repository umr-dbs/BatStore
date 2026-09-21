use crate::bat_page_model::BlockID;
use crate::bat_page_model::node::Node;
use crate::bat_sync::safe_cell::SafeCell;
use crate::bat_sync::smart_cell::SmartGuard;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::ptr::{addr_of, addr_of_mut};

pub struct Block<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> {
    // pub block_id: BlockID,
    pub node_data: SafeCell<Node<FAN_OUT, NUM_RECORDS, Key, Payload>>,
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> Clone for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn clone(&self) -> Self {
        Self {
            // block_id: self.block_id,
            node_data: SafeCell::new(self.node_data.as_ref().clone()),
        }
    }
}
impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> Default for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    fn default() -> Self {
        Block {
            // block_id: 0,
            node_data: SafeCell::new(Node::new_leaf()),
        }
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> Deref for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    type Target = Node<FAN_OUT, NUM_RECORDS, Key, Payload>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*addr_of!(self.node_data) as &Self::Target }
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> DerefMut for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *addr_of_mut!(self.node_data) as &mut Self::Target }
        // &mut self.node_data
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> AsRef<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    fn as_ref(&self) -> &Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe { &*addr_of!(self.node_data) as _ }
        // &self.node_data
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> AsMut<Node<FAN_OUT, NUM_RECORDS, Key, Payload>> for Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    #[inline(always)]
    fn as_mut(&mut self) -> &mut Node<FAN_OUT, NUM_RECORDS, Key, Payload> {
        unsafe { &mut *addr_of_mut!(self.node_data) as _ }
    }
}

pub type BlockGuard<'a, const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    SmartGuard<Block<FAN_OUT, NUM_RECORDS, Key, Payload>>;

impl<
    'a,
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>
{
}
