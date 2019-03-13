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
use super::kentry::KEntry;
use super::kbucket::KBucket;

/// KNodeTable Implementation
/// This uses a flattened approach whereby buckets are pre-allocated and indexed by their distance from the local Id
/// as a simplification of the allocation based branching approach introduced in the paper.
#[derive(Clone)]
pub struct KNodeTable<Id, Node> {
    id: Id,
    buckets: Arc<Mutex<Vec<Arc<Mutex<KBucket<Id, Node>>>>>>
}

impl <Id, Node> KNodeTable<Id, Node> 
where
    Id: DatabaseId + Clone + 'static,
    Node: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provded bucket and hash sizes
    pub fn new(id: Id, bucket_size: usize, hash_size: usize) -> KNodeTable<Id, Node> {
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
    fn bucket(&self, id: &Id) -> Arc<Mutex<KBucket<Id, Node>>> {
        let index = self.bucket_index(id);
        let buckets = &self.buckets.lock().unwrap();
        buckets[index].clone()
    }

    /// Fetch the bucket index for a given Id
    /// This is basically just the number of bits in the distance between the local and target Ids
    fn bucket_index(&self, id: &Id) -> usize {
        // Find difference
        let diff = KNodeTable::<Id, Node>::distance(&self.id, id);
        assert!(!diff.is_zero(), "Distance cannot be zero");

        let index = diff.bits() - 1;
        index
    }

    /// Fetch a specified entry
    pub (crate) fn entry(&self, id: &Id) -> Option<KEntry<Id, Node>> {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();
        bucket.find(id)
    }

    pub (crate) fn update_entry<F(&mut self, id: &Id, f: F)
    where F: FnMut(&mut KEntry<Id, Node>)
    {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();

        match bucket.find(id) {
            Some(e) => {
                f(&mut e);
                bucket.update(id, e);
            },
            None => ()
        };
    }
}

impl <Id, Node> NodeTable<Id, Node> for KNodeTable<Id, Node> 
where
    Id: DatabaseId + Clone + 'static,
    Node: Clone + Debug + 'static,
{
    /// Update a node in the table
    fn update(&mut self, id: &Id, node: Node) -> bool {
        if *id == self.id {
            return false
        }

        let bucket = self.bucket(id);
        let mut bucket = bucket.lock().unwrap();
        let mut node = node.clone();

        // TODO: reimplement seen
        //node.set_seen(Instant::now());

        bucket.update(id, node)
    }

     fn update_fn<T: FnMut(&Id, &mut Node)>(&mut self, id: &Id, f: T) {
        if *id == self.id {
            return;
        }

        // Fetch an instance from the bucket (and lock)
        let bucket = self.bucket(id);
        let mut bucket = bucket.lock().unwrap();

        // Check the instance exists
        match bucket.find(id) {
            Some(e) => f(id, &mut e.node()) ,
            None => return
        };

        

        //bucket.update(id, entry.node());
     }

    /// Find the nearest nodes to the provided Id in the given range
    fn nearest(&mut self, id: &Id, range: Range<usize>) -> Vec<(Id, Node)> {
        let buckets = self.buckets.lock().unwrap();

        // Create a list of all nodes
        let mut all: Vec<_> = buckets.iter().flat_map(|b| b.lock().unwrap().nodes() ).collect();
        let count = all.len();

        // Sort by distance
        all.sort_by_key(|e| { KNodeTable::<Id, Node>::distance(id, &e.id()) } );

        // Limit to count or total found
        let mut range = range;
        range.end = usize::min(count, range.end);

        let limited = all.drain(range).map(|e| (e.id(), e.node()) ).collect();
        limited
    }

    /// Peek at the oldest node in the bucket associated with a given Id
    fn peek_oldest(&mut self, id: &Id) -> Option<(Id, Node)> {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();
        bucket.oldest().map(|e| (e.id(), e.node()) )
    }

    fn replace(&mut self, id: Id, old: Node, new: Node) {
        let bucket = self.bucket(&id);
        let mut _bucket = bucket.lock().unwrap();

        unimplemented!();
    }

    /// Check if the node table contains a given node by Id
    /// This returns the node object if found
    fn contains(&self, id: &Id) -> Option<Node> {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();
        bucket.find(id).map(|e| e.node() )
    }
}



#[cfg(test)]
mod test {
    use crate::node::Node;
    use super::{KNodeTable, NodeTable};

    #[test]
    fn test_k_node_table() {
        let n = (0b0100, 1);

        let mut t = KNodeTable::<u64, u64>::new(n.0.clone(), 10, 4);

        let nodes = vec![
            (0b0000, 1),
            (0b0001, 2),
            (0b0110, 3),
            (0b1011, 4),
        ];

        // Add some nodes
        for n in &nodes {
            assert_eq!(true, t.contains(&n.0).is_none());
            assert_eq!(true, t.update(&n.0, n.1));
            assert_eq!(n.1, t.contains(&n.0).unwrap());
        }
        
        // Find closest nodes
        assert_eq!(vec![nodes[2].clone(), nodes[0].clone()], t.nearest(&n.0, 0..2));
        assert_eq!(vec![nodes[0].clone(), nodes[1].clone()], t.nearest(&0b0010, 0..2));
    }
}