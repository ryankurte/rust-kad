
use crate::id::DatabaseId;
use crate::node::Node;

pub enum UpdateResult<ID, ADDR> {
    Ok,
    Pending(Node<ID, ADDR>),
}

// Generic Node Table implementation
// This keeps track of known nodes
pub trait NodeTable<ID: DatabaseId, ADDR> {
    // Update a node in the table
    // Returns true if node has been stored or updated, false if there is no room remaining in the table
    fn update(&mut self, node: &Node<ID, ADDR>) -> bool;
    // Find nearest nodes
    // Returns a list of the nearest nodes to the provided id
    fn find_nearest(&mut self, id: &ID, count: usize) -> Vec<Node<ID, ADDR>>;
    // Peek at the oldest node from the k-bucket containing it
    // This returns an instance of the oldest node while leaving it in the appropriate bucket
    fn peek_oldest(&mut self, id: &ID) -> Option<Node<ID, ADDR>>;
    // Replace a node in a given bucket
    // This is used to replace an expired node in the bucket
    fn replace(&mut self, node: &Node<ID, ADDR>, replacement: &Node<ID, ADDR>);
}

// TODO: would it be better to remove expired nodes then re-call update?
// seems that deferring a swap should be more robust in the case of a split between decisions being made?

use std::fmt::Debug;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct KBucket<ID, ADDR> {
    bucket_size: usize,
    nodes: Arc<Mutex<VecDeque<Node<ID, ADDR>>>>,
}

impl <ID, ADDR> KBucket<ID, ADDR> 
where
    ID: DatabaseId,
    ADDR: Clone + Debug,
{
    /// Create a new KBucket with the given size
    pub fn new(bucket_size: usize) -> KBucket<ID, ADDR> {
        KBucket{bucket_size, nodes: Arc::new(Mutex::new(VecDeque::with_capacity(bucket_size)))}
    }

    /// Update a node in the KBucket
    pub fn update(&mut self, node: &Node<ID, ADDR>) -> bool {
        let nodes = self.nodes.lock().unwrap();
        if let Some(_n) = nodes.iter().find(|n| n.id == node.id) {
            // If the node already exists, update it
            self.update_position(node);
            info!("[KBucket] Updated node {:?}", node);
            true
        } else if nodes.len() < self.bucket_size {
            // If there's space in the bucket, add it
            self.nodes.lock().unwrap().push_back(node.clone());
            info!("[KBucket] Added node {:?}", node);
            true
        } else {
            // If there's no space, discard it
            info!("[KBucket] No space to add node {:?}", node);
            false
        }
    }

    /// Move a node to the start of the array
    pub fn update_position(&mut self, node: &Node<ID, ADDR>) {
        let nodes = *self.nodes.lock().unwrap();
        // Find the node to update
        if let Some(_existing) = nodes.iter().find(|n| n.id == node.id).map(|n| n.clone()) {
            // Update node array
            nodes = nodes.iter().filter(|n| n.id != node.id).map(|n| n.clone()).collect();
            // Push node to front
            nodes.push_back(node.clone());
        }
    }
}

pub struct KNodeTable<ID, ADDR> {
    id: ID,
    bucket_size: usize,
    hash_size: usize,
    buckets: Vec<KBucket<ID, ADDR>>
}

impl <ID, ADDR> KNodeTable<ID, ADDR> 
where
    ID: DatabaseId + Clone,
    ADDR: Clone + Debug,
{
    pub fn new(id: ID, bucket_size: usize, hash_size: usize) -> KNodeTable<ID, ADDR> {
        KNodeTable{id, bucket_size, hash_size, buckets: vec![KBucket::new(bucket_size)]}
    }

    pub fn distance(a: &ID, b: &ID) -> ID {
        ID::xor(a, b)
    }

    pub fn update(id: &ID, addr: &ADDR) -> bool {
        let bucket = self.bucket_mut(id);
    }

    fn bucket_index(&self, id: &ID) -> usize {
        // Find difference
        let diff = KNodeTable::<ID, ADDR>::distance(&self.id, id);
        ID::bits(&diff) - 1
    }

    //pub fn bucket_mut(&mut self, id: &ID) -> &mut KBucket<ID, ADDR> {
        
    //}

}

#[cfg(test)]
mod test {
    use super::{KBucket, KNodeTable};

    #[test]
    fn test() {
        let mut k = KNodeTable::<u64, u64>::new(0b0010, 2, 4);

        k.update()

    }
}