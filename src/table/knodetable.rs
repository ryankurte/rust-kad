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

use crate::common::{DatabaseId, Entry};

use super::nodetable::{NodeTable, BucketInfo};
use super::kbucket::KBucket;

/// KNodeTable Implementation
/// This uses a flattened approach whereby buckets are pre-allocated and indexed by their distance from the local Id
/// as a simplification of the allocation based branching approach introduced in the paper.
#[derive(Clone)]
pub struct KNodeTable<Id, Info> {
    id: Id,
    buckets: Arc<Mutex<Vec<Arc<Mutex<KBucket<Id, Info>>>>>>
}



impl <Id, Info> KNodeTable<Id, Info> 
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provded bucket and hash sizes
    pub fn new(id: Id, bucket_size: usize, hash_size: usize) -> KNodeTable<Id, Info> {
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
    fn bucket(&self, id: &Id) -> Arc<Mutex<KBucket<Id, Info>>> {
        let index = self.bucket_index(id);
        let buckets = &self.buckets.lock().unwrap();
        buckets[index].clone()
    }

    /// Fetch the bucket index for a given Id
    /// This is basically just the number of bits in the distance between the local and target Ids
    fn bucket_index(&self, id: &Id) -> usize {
        // Find difference
        let diff = KNodeTable::<Id, Info>::distance(&self.id, id);
        assert!(!diff.is_zero(), "Distance cannot be zero");

        let index = diff.bits() - 1;
        index
    }

    #[allow(dead_code)]
    fn update_buckets(&self) {
        let _buckets = self.buckets.lock().unwrap();

        unimplemented!()
    }
}

impl <Id, Info> NodeTable<Id, Info> for KNodeTable<Id, Info> 
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create or update a node in the NodeTable
    fn create_or_update(&mut self, node: &Entry<Id, Info>) -> bool {
        if node.id() == &self.id {
            return false
        }

        let bucket = self.bucket(node.id());
        let mut bucket = bucket.lock().unwrap();
        let mut node = node.clone();
        node.set_seen(Instant::now());
        bucket.create_or_update(&node)
    }

    /// Find the nearest nodes to the provided Id in the given range
    fn nearest(&self, id: &Id, range: Range<usize>) -> Vec<Entry<Id, Info>> {
        let buckets = self.buckets.lock().unwrap();

        // Create a list of all nodes
        let mut all: Vec<_> = buckets.iter().flat_map(|b| b.lock().unwrap().nodes() ).collect();
        let count = all.len();

        // Sort by distance
        all.sort_by_key(|n| { KNodeTable::<Id, Info>::distance(id, n.id()) } );

        // Limit to count or total found
        let mut range = range;
        range.end = usize::min(count, range.end);

        let limited = all.drain(range).collect();
        limited
    }

    /// Check if the node NodeTable contains a given node by Id
    /// This returns the node object if found
    fn contains(&self, id: &Id) -> Option<Entry<Id, Info>> {
        let bucket = self.bucket(id);
        let bucket = bucket.lock().unwrap();
        bucket.find(id)
    }

    /// Peek at the oldest node in the bucket associated with a given Id
    fn iter_oldest(&self) -> Box<dyn Iterator<Item=Entry<Id, Info>>> {
        Box::new(KNodeTableIterOldest{index: 0, buckets: self.buckets.clone() })
    }

    /// Update an entry by ID
    fn update_entry<F>(&mut self, id: &Id, f: F) -> bool 
    where F: Fn(&mut Entry<Id, Info>)
    {
        let bucket = self.bucket(id);
        let mut bucket = bucket.lock().unwrap();
        bucket.update_entry(id, f)
    }

    /// Remove an entry by ID
    fn remove_entry(&mut self, id: &Id) {
        let bucket = self.bucket(id);
        let mut bucket = bucket.lock().unwrap();
        bucket.remove_entry(id, false);
    }

    /// Fetch information from each bucket
    fn bucket_info(&self) -> Vec<BucketInfo> {
        let buckets = self.buckets.lock().unwrap();
        let mut info = Vec::with_capacity(buckets.len());

        for i in 0..buckets.len() {
            let b = buckets[i].lock().unwrap();
            info.push(BucketInfo{
                index: i, 
                nodes: b.node_count(), 
                updated: b.updated()
            });
        }
        info
    }
}

/// Iterator over the oldest node from each bucket
#[derive(Clone)]
pub struct KNodeTableIterOldest<Id, Info> {
    index: usize,
    buckets: Arc<Mutex<Vec<Arc<Mutex<KBucket<Id, Info>>>>>>,
}

impl <Id, Info> Iterator for KNodeTableIterOldest<Id, Info> 
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    type Item = Entry<Id, Info>;

    fn next(&mut self) -> Option<Self::Item> {
        let buckets = self.buckets.lock().unwrap();
        let mut r = None;

        while self.index < buckets.len() {
            let bucket = buckets[self.index].lock().unwrap();
            r = bucket.oldest();

            self.index += 1;

            if let Some(_) = &r {
                break
            }
        }

        return r
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use super::{KNodeTable, NodeTable};

    #[test]
    fn test_k_node_table() {
        let n = Entry::new([0b0100], 1);

        let mut t = KNodeTable::<[u8; 1], u64>::new(n.id().clone(), 10, 4);

        let nodes = vec![
            Entry::new([0b0000], 1),
            Entry::new([0b0001], 2),
            Entry::new([0b0110], 3),
            Entry::new([0b1011], 4),
        ];

        // Add some nodes
        for n in &nodes {
            assert_eq!(true, t.contains(n.id()).is_none());
            assert_eq!(true, t.create_or_update(&n));
            assert_eq!(*n, t.contains(n.id()).unwrap());
        }
        
        // Find closest nodes
        assert_eq!(vec![nodes[2].clone(), nodes[0].clone()], t.nearest(n.id(), 0..2));
        assert_eq!(vec![nodes[0].clone(), nodes[1].clone()], t.nearest(&[0b0010], 0..2));
    }
}