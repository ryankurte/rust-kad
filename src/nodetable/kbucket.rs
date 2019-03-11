/**
 * rust-kad
 * Kademlia KBucket implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use crate::id::DatabaseId;
use crate::node::Node;

use std::fmt::Debug;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// KBucket implementation
/// This implements a single bucket for use in the KNodeTable implementation
pub struct KBucket<Id, Node> {
    bucket_size: usize,
    entries: Arc<Mutex<VecDeque<KEntry<Id, Node>>>>,
    pending: Option<KEntry<Id, Node>>,
    updated: Option<Instant>,
}

#[derive(Clone, Debug, PartialEq)]
struct KEntry<Id, Node> {
    id: Id,
    node: Node,
}

impl <Id, Node> KEntry<Id, Node> {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn node(&self) -> Node {
        self.node
    }
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
            updated: None}
    }

    /// Update a node in the bucket
    pub fn update(&mut self, id: Id, node: Node) -> bool {
        let mut entries = self.entries.lock().unwrap();
        let entry = KEntry{id, node};

        let res = if let Some(_n) = entries.clone().iter().find(|n| n.id() == id ) {
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
        let mut b = KBucket::<u64, u64>::new(4);

        assert_eq!(true, b.find(&0b00).is_none());

        // Generate fake nodes
        let n1 = Node::new(0b00, 1);
        let n2 = Node::new(0b01, 2);
        let n3 = Node::new(0b10, 3);
        let n4 = Node::new(0b11, 4);

        // Fill KBucket
        assert_eq!(true, b.update(&n1));
        assert_eq!(n1, b.find(n1.id()).unwrap().node);
        
        assert_eq!(true, b.update(&n2));
        assert_eq!(n2, b.find(n2.id()).unwrap().node);
        
        assert_eq!(true, b.update(&n3));
        assert_eq!(n3, b.find(n3.id()).unwrap().node);

        assert_eq!(true, b.update(&n4));
        assert_eq!(n4, b.find(n4.id()).unwrap().node);

        // Attempt to add to full KBucket
        assert_eq!(false, b.update(&Node::new(0b100, 5)));

        // Update existing item
        let mut n4a = n4.clone();
        n4a.set_address(&5);
        assert_eq!(true, b.update(&n4a));
        assert_eq!(n4a, b.find(n4.id()).unwrap());
    }
}