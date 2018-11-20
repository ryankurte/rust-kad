
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

/// KBucket implementation
/// This implements a single bucket for use in the KNodeTable implementation
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
        let mut nodes = self.nodes.lock().unwrap();

        if let Some(_n) = nodes.iter().find(|n| n.id == node.id) {
            // If the node already exists, update it
            info!("[KBucket] Updating node {:?}", node);
            KBucket::update_position(&mut nodes, node);
            true
        } else if nodes.len() < self.bucket_size {
            // If there's space in the bucket, add it
            info!("[KBucket] Adding node {:?}", node);
            nodes.push_back(node.clone());
            true
        } else {
            // If there's no space, discard it
            info!("[KBucket] No space to add node {:?}", node);
            false
        }
    }

    /// Move a node to the start of the array
    fn update_position(nodes: &mut VecDeque<Node<ID, ADDR>>, node: &Node<ID, ADDR>) {
        // Find the node to update
        if let Some(_existing) = nodes.iter().find(|n| n.id == node.id).map(|n| n.clone()) {
            // Update node array
            *nodes = nodes.iter().filter(|n| n.id != node.id).map(|n| n.clone()).collect();
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
    /// Create a new KNodeTable with the provded bucket and hash sizes
    pub fn new(node: &Node<ID, ADDR>, bucket_size: usize, hash_size: usize) -> KNodeTable<ID, ADDR> {
        // Create a new bucket and assign own ID
        let mut bucket = KBucket::new(bucket_size);
        bucket.update(node);
        // Generate KNodeTable object
        KNodeTable{id: node.id.clone(), bucket_size, hash_size, buckets: vec![bucket]}
    }

    // Calculate the distance between two IDs.
    pub fn distance(a: &ID, b: &ID) -> ID {
        ID::xor(a, b)
    }

    /// Update a node in the KNodeTable
    pub fn update(&mut self, node: Node<ID, ADDR>) -> bool {
        let bucket = self.bucket_mut(&node.id);
        bucket.update(&node)
    }

    /// Fetch a mutable reference to the bucket containing the provided ID
    pub fn bucket_mut(&mut self, id: &ID) -> &mut KBucket<ID, ADDR> {
        let index = self.bucket_index(id);
        &mut self.buckets[index]
    }


    fn bucket_index(&self, id: &ID) -> usize {
        // Find difference
        let diff = KNodeTable::<ID, ADDR>::distance(&self.id, id);
        ID::bits(&diff) - 1
    }
}

#[cfg(test)]
mod test {
    use crate::node::Node;
    use super::{KBucket, KNodeTable};

    #[test]
    fn test_k_bucket() {
        println!("Here");
        let mut b = KBucket::<u64, u64>::new(4);

        // Fill KBucket
        assert_eq!(true, b.update(&Node{id: 0b00, address: 1}));
        assert_eq!(true, b.update(&Node{id: 0b01, address: 2}));
        assert_eq!(true, b.update(&Node{id: 0b10, address: 3}));
        assert_eq!(true, b.update(&Node{id: 0b11, address: 4}));

        // Attempt to add to full KBucket
        assert_eq!(false, b.update(&Node{id: 0b100, address: 5}));

        // Update existing item
        assert_eq!(true, b.update(&Node{id: 0b00, address: 6}));
    }

    #[test]
    fn test_k_node_table() {
        let mut t = KNodeTable::<u64, u64>::new(&Node{id: 0b0011, address: 1}, 2, 0);


    }
}