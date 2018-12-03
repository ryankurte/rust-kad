/**
 * rust-kad
 * Kademlia Node Table Implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::cmp;
use std::ops::Range;
use std::fmt::Debug;
use std::time::Instant;

use crate::id::DatabaseId;
use crate::node::Node;

use super::nodetable::NodeTable;
use super::kbucket::KBucket;

pub struct KNodeTable<ID, ADDR> {
    id: ID,
    buckets: Vec<KBucket<ID, ADDR>>
}

impl <ID, ADDR> KNodeTable<ID, ADDR> 
where
    ID: DatabaseId + Clone + 'static,
    ADDR: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provded bucket and hash sizes
    pub fn new(node: &Node<ID, ADDR>, bucket_size: usize, hash_size: usize) -> KNodeTable<ID, ADDR> {
        // Create a new bucket and assign own ID
        let buckets = (0..hash_size).map(|_| KBucket::new(bucket_size)).collect();
        // Generate KNodeTable object
        KNodeTable{id: node.id().clone(), buckets: buckets}
    }

    /// Find a given node in the table
    pub fn find(&mut self, id: &ID) -> Option<Node<ID, ADDR>> {
        let bucket = self.bucket_mut(id);
        bucket.find(id)
    }

    // Calculate the distance between two IDs.
    fn distance(a: &ID, b: &ID) -> ID {
        ID::xor(a, b)
    }

    /// Fetch a mutable reference to the bucket containing the provided ID
    fn bucket_mut(&mut self, id: &ID) -> &mut KBucket<ID, ADDR> {
        let index = self.bucket_index(id);
        &mut self.buckets[index]
    }

    /// Fetch a mutable reference to the bucket containing the provided ID
    fn bucket(&self, id: &ID) -> &KBucket<ID, ADDR> {
        let index = self.bucket_index(id);
        &self.buckets[index]
    }

    fn bucket_index(&self, id: &ID) -> usize {
        // Find difference
        let diff = KNodeTable::<ID, ADDR>::distance(&self.id, id);
        assert!(!diff.is_zero(), "Distance cannot be zero");

        let index = diff.bits() - 1;
        index
    }
}

impl <ID, ADDR> NodeTable<ID, ADDR> for KNodeTable<ID, ADDR> 
where
    ID: DatabaseId + Clone + 'static,
    ADDR: Clone + Debug + 'static,
{
    /// Update a node in the table
    fn update(&mut self, node: &Node<ID, ADDR>) -> bool {
        let bucket = self.bucket_mut(node.id());
        let mut node = node.clone();
        node.set_seen(Instant::now());
        bucket.update(&node)
    }

    /// Find the nearest nodes to the provided ID in the given range
    fn nearest(&mut self, id: &ID, range: Range<usize>) -> Vec<Node<ID, ADDR>> {

        // Create a list of all nodes
        let mut all: Vec<_> = self.buckets.iter().flat_map(|b| b.nodes() ).collect();
        let count = all.len();

        // Sort by distance
        all.sort_by_key(|n| { KNodeTable::<ID, ADDR>::distance(id, n.id()) } );

        // Limit to count or total found
        let mut range = range;
        if range.end > count {
            range.end = count;
        }

        let limited = all.drain(range).collect();
        limited
    }

    /// Peek at the oldest node in the bucket associated with a given ID
    fn peek_oldest(&mut self, id: &ID) -> Option<Node<ID, ADDR>> {
        let bucket = self.bucket(id);
        bucket.oldest()
    }

    fn replace(&mut self, node: &Node<ID, ADDR>, _replacement: &Node<ID, ADDR>) {
        let _bucket = self.bucket_mut(node.id());


    }

    /// Check if the node table contains a given node by ID
    /// This returns the node object if found
    fn contains(&self, id: &ID) -> Option<Node<ID, ADDR>> {
        let bucket = self.bucket(id);
        bucket.find(id)
    }
}

#[cfg(test)]
mod test {
    use crate::node::Node;
    use super::{KNodeTable, NodeTable};

    #[test]
    fn test_k_node_table() {
        let n = Node::new(0b0100, 1);

        let mut t = KNodeTable::<u64, u64>::new(&n, 10, 4);

        let nodes = vec![
            Node::new(0b0000, 1),
            Node::new(0b0001, 2),
            Node::new(0b0110, 3),
            Node::new(0b1011, 4),
        ];

        // Add some nodes
        for n in &nodes {
            assert_eq!(true, t.find(n.id()).is_none());
            assert_eq!(true, t.update(&n));
            assert_eq!(*n, t.find(n.id()).unwrap());
        }
        
        // Find closest nodes
        assert_eq!(vec![nodes[2].clone(), nodes[0].clone()], t.nearest(n.id(), 0..2));
        assert_eq!(vec![nodes[0].clone(), nodes[1].clone()], t.nearest(&0b0010, 0..2));
    }
}