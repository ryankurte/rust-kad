/**
 * rust-kad
 * Kademlia Node Table Implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::ops::Range;
use std::fmt::Debug;
use std::time::Instant;
use std::sync::{Arc, Mutex};

use crate::id::DatabaseId;
use crate::node::Node;

use super::nodetable::NodeTable;
use super::kbucket::KBucket;

/// KNodeTable Implementation
/// This uses a flattened approach whereby buckets are pre-allocated and indexed by their distance from the local Id
/// as a simplification of the allocation based branching approach introduced in the paper.
#[derive(Clone)]
pub struct KNodeTable<Id, Addr> {
    id: Id,
    buckets: Arc<Mutex<Vec<Arc<Mutex<KBucket<Id, Addr>>>>>>
}

impl <Id, Addr> KNodeTable<Id, Addr> 
where
    Id: DatabaseId + Clone + 'static,
    Addr: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provded bucket and hash sizes
    pub fn new(id: Id, bucket_size: usize, hash_size: usize) -> KNodeTable<Id, Addr> {
        // Create a new bucket and assign own Id
        let buckets = (0..hash_size).map(|_| Arc::new(Mutex::new(KBucket::new(bucket_size)))).collect();
        // Generate KNodeTable object
        KNodeTable{id, buckets: Arc::new(Mutex::new(buckets)) }
    }

    // Calculate the distance between two Ids.
    fn distance(a: &Id, b: &Id) -> Id {
        Id::xor(a, b)
    }

    /// Fetch a reference to the bucket containing the provided Id
    fn bucket(&self, id: &Id) -> Arc<Mutex<KBucket<Id, Addr>>> {
        let index = self.bucket_index(id);
        let buckets = &self.buckets.lock().unwrap();
        buckets[index].clone()
    }

    /// Fetch the bucket index for a given Id
    /// This is basically just the number of bits in the distance between the local and target Ids
    fn bucket_index(&self, id: &Id) -> usize {
        // Find difference
        let diff = KNodeTable::<Id, Addr>::distance(&self.id, id);
        assert!(!diff.is_zero(), "Distance cannot be zero");

        let index = diff.bits() - 1;
        index
    }
}

impl <Id, Addr> NodeTable<Id, Addr> for KNodeTable<Id, Addr> 
where
    Id: DatabaseId + Clone + 'static,
    Addr: Clone + Debug + 'static,
{
    /// Update a node in the table
    fn update(&mut self, node: &Node<Id, Addr>) -> bool {
        if node.id() == &self.id {
            return false
        }

        let bucket = self.bucket(node.id());
        let mut bucket = bucket.lock().unwrap();
        let mut node = node.clone();
        node.set_seen(Instant::now());
        bucket.update(&node)
    }

    /// Find the nearest nodes to the provided Id in the given range
    fn nearest(&mut self, id: &Id, range: Range<usize>) -> Vec<Node<Id, Addr>> {
        let buckets = self.buckets.lock().unwrap();

        // Create a list of all nodes
        let mut all: Vec<_> = buckets.iter().flat_map(|b| b.lock().unwrap().nodes() ).collect();
        let count = all.len();

        // Sort by distance
        all.sort_by_key(|n| { KNodeTable::<Id, Addr>::distance(id, n.id()) } );

        // Limit to count or total found
        let mut range = range;
        range.end = usize::min(count, range.end);

        let limited = all.drain(range).collect();
        limited
    }

    /// Peek at the oldest node in the bucket associated with a given Id
    fn peek_oldest(&mut self, id: &Id) -> Option<Node<Id, Addr>> {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();
        bucket.oldest()
    }

    fn replace(&mut self, node: &Node<Id, Addr>, _replacement: &Node<Id, Addr>) {
        let bucket = self.bucket(node.id());
        let mut _bucket = bucket.lock().unwrap();


    }

    /// Check if the node table contains a given node by Id
    /// This returns the node object if found
    fn contains(&self, id: &Id) -> Option<Node<Id, Addr>> {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();
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

        let mut t = KNodeTable::<u64, u64>::new(n.id().clone(), 10, 4);

        let nodes = vec![
            Node::new(0b0000, 1),
            Node::new(0b0001, 2),
            Node::new(0b0110, 3),
            Node::new(0b1011, 4),
        ];

        // Add some nodes
        for n in &nodes {
            assert_eq!(true, t.contains(n.id()).is_none());
            assert_eq!(true, t.update(&n));
            assert_eq!(*n, t.contains(n.id()).unwrap());
        }
        
        // Find closest nodes
        assert_eq!(vec![nodes[2].clone(), nodes[0].clone()], t.nearest(n.id(), 0..2));
        assert_eq!(vec![nodes[0].clone(), nodes[1].clone()], t.nearest(&0b0010, 0..2));
    }
}