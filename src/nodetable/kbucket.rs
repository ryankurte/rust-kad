/**
 * rust-kad
 * Kademlia KBucket implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use crate::id::DatabaseId;

use std::fmt::Debug;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::kentry::KEntry;

/// KBucket implementation
/// This implements a single bucket for use in the KNodeTable implementation
pub struct KBucket<Id, Node> {
    bucket_size: usize,
    entries: Arc<Mutex<VecDeque<KEntry<Id, Node>>>>,
    pending: Option<KEntry<Id, Node>>,
    updated: Option<Instant>,
}

impl <Id, Node> KBucket<Id, Node> 
where
    Id: DatabaseId + 'static,
    Node: Clone + Debug + 'static,
{
    /// Create a new KBucket with the given size
    pub fn new(bucket_size: usize) -> KBucket<Id, Node> {
        KBucket{bucket_size, 
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(bucket_size))), 
            pending: None, 
            updated: None
        }
    }

    /// Update a node in the bucket
    pub fn update(&mut self, id: &Id, entry: KEntry<Id, Node>) -> bool {
        let mut entries = self.entries.lock().unwrap();

        let res = if let Some(_n) = entries.clone().iter().find(|n| n.id() == *id ) {
            // If the node already exists, update it
            info!(target: "dht", "[KBucket] Updating node {:?}", entry);
            KBucket::update_position(&mut entries, &entry);
            true
        } else if entries.len() < self.bucket_size {
            // If there's space in the bucket, add it
            info!(target: "dht", "[KBucket] Adding node {:?}", entry);
            entries.push_back(entry);
            true
        } else {
            // If there's no space, discard it
            info!(target: "dht", "[KBucket] No space to add node {:?}", entry);
            self.pending = Some(entry);
            false
        };

        if res {
            self.updated = Some(Instant::now());
        }

        res
    }

    /// Find a node in the bucket
    pub fn find(&self, id: &Id) -> Option<KEntry<Id, Node>> {
        self.entries.lock().unwrap().iter().find(|e| e.id() == *id).map(|e| e.clone())
    }

    /// Clone the list of nodes currently in the bucket
    pub(crate) fn nodes(&self) -> Vec<KEntry<Id, Node>> {
        self.entries.lock().unwrap().iter().map(|n| n.clone()).collect()
    }

    /// Fetch the oldest node in the bucket
    pub(crate) fn oldest(&self) -> Option<KEntry<Id, Node>> {
        let entries = self.entries.lock().unwrap();
        entries.get(entries.len()-1).map(|n| n.clone())
    }

    /// Move a node to the start of the bucket
    fn update_position(entries: &mut VecDeque<KEntry<Id, Node>>, entry: &KEntry<Id, Node>) {
        // Find the node to update
        if let Some(_existing) = entries.iter().find(|e| e.id() == entry.id()).map(|e| e.clone()) {
            // Update node array
            *entries = entries.iter().filter(|e| e.id() != entry.id()).map(|e| e.clone()).collect();
            // Push node to front
            entries.push_back(entry.clone());
        }
    }
}

#[cfg(test)]
mod test {
    use crate::node::Node;
    use super::{KBucket, KEntry};

    #[test]
    fn test_k_bucket_update() {
        let mut b = KBucket::<u64, u16>::new(4);

        assert_eq!(true, b.find(&0b00).is_none());

        // Generate fake nodes
        let (a1, n1) = (0b000, 1);
        let (a2, n2) = (0b001, 2);
        let (a3, n3) = (0b010, 3);
        let (a4, n4) = (0b011, 4);
        let (a5, n5) = (0b100, 5);

        // Fill KBucket
        assert_eq!(true, b.update(&a1, n1));
        assert_eq!(n1, b.find(&a1).unwrap().node());
        
        assert_eq!(true, b.update(&a2, n2));
        assert_eq!(n2, b.find(&a2).unwrap().node());
        
        assert_eq!(true, b.update(&a3, n3));
        assert_eq!(n3, b.find(&a3).unwrap().node());

        assert_eq!(true, b.update(&a4, n4));
        assert_eq!(n4, b.find(&a4).unwrap().node());

        // Attempt to add to full KBucket
        assert_eq!(false, b.update(&a5, n5));

        // Update existing item
        let mut n4a = 100;
        assert_eq!(true, b.update(&a4, n4a));
        assert_eq!(n4a, b.find(&a4).unwrap().node());
    }
}