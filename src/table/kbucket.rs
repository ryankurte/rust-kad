/**
 * rust-kad
 * Kademlia KBucket implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use crate::common::{DatabaseId, Entry};

use std::fmt::Debug;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// KBucket implementation
/// This implements a single bucket for use in the KNodeTable implementation
pub struct KBucket<Id, Info> {
    bucket_size: usize,
    nodes: Arc<Mutex<VecDeque<Entry<Id, Info>>>>,
    pending: Option<Entry<Id, Info>>,
    updated: Option<Instant>,
}

impl <Id, Info> KBucket<Id, Info> 
where
    Id: DatabaseId + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create a new KBucket with the given size
    pub fn new(bucket_size: usize) -> KBucket<Id, Info> {
        KBucket{bucket_size, 
            nodes: Arc::new(Mutex::new(VecDeque::with_capacity(bucket_size))), 
            pending: None, 
            updated: None}
    }

    /// Update a node in the bucket
    pub fn update(&mut self, node: &Entry<Id, Info>) -> bool {
        let mut nodes = self.nodes.lock().unwrap();

        let res = if let Some(_n) = nodes.clone().iter().find(|n| n.id() == node.id()) {
            // If the node already exists, update it
            info!(target: "dht", "[KBucket] Updating node {:?}", node);
            KBucket::update_position(&mut nodes, node);
            true
        } else if nodes.len() < self.bucket_size {
            // If there's space in the bucket, add it
            info!(target: "dht", "[KBucket] Adding node {:?}", node);
            nodes.push_back(node.clone());
            true
        } else {
            // If there's no space, discard it
            info!(target: "dht", "[KBucket] No space to add node {:?}", node);
            self.pending = Some(node.clone());
            false
        };

        if res {
            self.updated = Some(Instant::now());
        }

        res
    }

    /// Find a node in the bucket
    pub fn find(&self, id: &Id) -> Option<Entry<Id, Info>> {
        self.nodes.lock().unwrap().iter().find(|n| *n.id() == *id).map(|n| n.clone())
    }

    /// Clone the list of nodes currently in the bucket
    pub(crate) fn nodes(&self) -> Vec<Entry<Id, Info>> {
        self.nodes.lock().unwrap().iter().map(|n| n.clone()).collect()
    }

    /// Fetch the oldest node in the bucket
    pub(crate) fn oldest(&self) -> Option<Entry<Id, Info>> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(nodes.len()-1).map(|n| n.clone())
    }

    /// Move a node to the start of the bucket
    fn update_position(nodes: &mut VecDeque<Entry<Id, Info>>, node: &Entry<Id, Info>) {
        // Find the node to update
        if let Some(_existing) = nodes.iter().find(|n| n.id() == node.id()).map(|n| n.clone()) {
            // Update node array
            *nodes = nodes.iter().filter(|n| n.id() != node.id()).map(|n| n.clone()).collect();
            // Push node to front
            nodes.push_back(node.clone());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_k_bucket_update() {
        let mut b = KBucket::<u64, u64>::new(4);

        assert_eq!(true, b.find(&0b00).is_none());

        // Generate fake nodes
        let n1 = Entry::new(0b00, 1);
        let n2 = Entry::new(0b01, 2);
        let n3 = Entry::new(0b10, 3);
        let n4 = Entry::new(0b11, 4);

        // Fill KBucket
        assert_eq!(true, b.update(&n1));
        assert_eq!(n1, b.find(n1.id()).unwrap());
        
        assert_eq!(true, b.update(&n2));
        assert_eq!(n2, b.find(n2.id()).unwrap());
        
        assert_eq!(true, b.update(&n3));
        assert_eq!(n3, b.find(n3.id()).unwrap());

        assert_eq!(true, b.update(&n4));
        assert_eq!(n4, b.find(n4.id()).unwrap());

        // Attempt to add to full KBucket
        assert_eq!(false, b.update(&Entry::new(0b100, 5)));

        // Update existing item
        let mut n4a = n4.clone();
        n4a.set_info(&5);
        assert_eq!(true, b.update(&n4a));
        assert_eq!(n4a, b.find(n4.id()).unwrap());
    }
}