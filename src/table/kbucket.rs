/**
 * rust-kad
 * Kademlia KBucket implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use crate::common::{DatabaseId, Entry};

use std::collections::VecDeque;
use std::fmt::Debug;
use std::time::Instant;

use log::trace;

/// KBucket implementation
/// This implements a single bucket for use in the KNodeTable implementation
pub struct KBucket<Id, Info> {
    bucket_size: usize,
    nodes: VecDeque<Entry<Id, Info>>,
    pending: Option<Entry<Id, Info>>,
    updated: Option<Instant>,
}

impl<Id, Info> KBucket<Id, Info>
where
    Id: DatabaseId + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create a new KBucket with the given size
    pub fn new(bucket_size: usize) -> KBucket<Id, Info> {
        KBucket {
            bucket_size,
            nodes: VecDeque::with_capacity(bucket_size),
            pending: None,
            updated: None,
        }
    }

    /// Update a node in the bucket
    /// TODO: positional updating seems inefficient and complex vs. just sorting by last_seen..?
    pub fn create_or_update(&mut self, node: &Entry<Id, Info>) -> bool {

        let res = if let Some(_n) = self.nodes.clone().iter().find(|n| n.id() == node.id()) {
            // If the node already exists, update it
            trace!(target: "dht", "[KBucket] Updating node {:?}", node);
            KBucket::update_position(&mut self.nodes, node);
            true

        } else if self.nodes.len() < self.bucket_size {
            // If there's space in the bucket, add it
            trace!(target: "dht", "[KBucket] Adding node {:?}", node);
            self.nodes.push_front(node.clone());
            true

        } else {
            // If there's no space, discard it
            trace!(target: "dht", "[KBucket] No space to add node {:?}", node);
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
        self.nodes
            .iter()
            .find(|n| *n.id() == *id)
            .map(|n| n.clone())
    }

    /// Clone the list of nodes currently in the bucket
    pub fn nodes(&self) -> Vec<Entry<Id, Info>> {
        self.nodes
            .iter()
            .map(|n| n.clone())
            .collect()
    }

    /// Fetch last updated time
    pub fn updated(&self) -> Option<Instant> {
        self.updated
    }

    /// Fetch number of nodes in bucket
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Fetch the oldest node in the bucket
    pub fn oldest(&self) -> Option<Entry<Id, Info>> {
        if self.nodes.len() == 0 {
            return None;
        }
        self.nodes.get(self.nodes.len() - 1).map(|n| n.clone())
    }

    /// Move a node to the start of the bucket
    fn update_position(nodes: &mut VecDeque<Entry<Id, Info>>, node: &Entry<Id, Info>) {
        // Find matching node by index
        let found = nodes.iter().enumerate()
            .find_map(|(i, n)| {
                if n.id() == node.id() {
                    Some(i)
                } else {
                    None
                }
            });

        // Nothing to do if the node is not found
        let i = match found {
            Some(v) => v,
            None => return,
        };

        // Remove prior node entry from list
        nodes.remove(i);

        // Push updated node entry to front
        nodes.push_front(node.clone());
    }

    /// Update an entry by ID
    pub fn update_entry<F>(&mut self, id: &Id, f: F) -> bool
    where
        F: Fn(&mut Entry<Id, Info>),
    {
        if let Some(ref mut n) = self.nodes.iter_mut().find(|n| n.id() == id) {
            (f)(n);
            return true;
        }
        false
    }

    /// Remove an entry from the bucket
    pub fn remove_entry(&mut self, id: &Id, replace: bool) {
        // Find matching node by index
        let index = self.nodes.iter().enumerate()
            .find_map(|(i, n)| {
                if n.id() == id {
                    Some(i)
                } else {
                    None
                }
            });

        // Nothing to do if the node is not found
        let index = match index {
            Some(i) => i,
            None => return,
        };

        // If we have a viable index, remove the node
        self.nodes.remove(index);

        // Replace with pending node if enabled and found
        if replace && self.nodes.len() < self.bucket_size && self.pending.is_some() {
            self.nodes.push_back(self.pending.take().unwrap());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_k_bucket_update() {
        let mut b = KBucket::<[u8; 1], u64>::new(4);

        assert_eq!(true, b.find(&[0b0000]).is_none());

        // Generate fake nodes
        let n1 = Entry::new([0b0000], 1);
        let n2 = Entry::new([0b0001], 2);
        let n3 = Entry::new([0b0010], 3);
        let n4 = Entry::new([0b0011], 4);
        let n5 = Entry::new([0b0100], 5);

        // Fill KBucket
        assert_eq!(true, b.create_or_update(&n1));
        assert_eq!(n1, b.find(n1.id()).unwrap());

        assert_eq!(true, b.create_or_update(&n2));
        assert_eq!(n2, b.find(n2.id()).unwrap());

        assert_eq!(true, b.create_or_update(&n3));
        assert_eq!(n3, b.find(n3.id()).unwrap());

        assert_eq!(true, b.create_or_update(&n4));
        assert_eq!(n4, b.find(n4.id()).unwrap());

        // Attempt to add to full KBucket
        assert_eq!(false, b.create_or_update(&n5));

        // Check ordering
        assert_eq!(
            b.nodes(),
            vec![n4.clone(), n3.clone(), n2.clone(), n1.clone()]
        );

        // Update node
        assert_eq!(true, b.create_or_update(&n1));

        // Check new ordering
        assert_eq!(
            b.nodes(),
            vec![n1.clone(), n4.clone(), n3.clone(), n2.clone()]
        );

        // Update existing item
        let mut n4a = n4.clone();
        n4a.set_info(&5);
        assert_eq!(true, b.create_or_update(&n4a));
        assert_eq!(n4a, b.find(n4.id()).unwrap());

        // Check new ordering
        assert_eq!(
            b.nodes(),
            vec![n4a.clone(), n1.clone(), n3.clone(), n2.clone()]
        );

        // Remote node and replace with pending
        b.remove_entry(n1.id(), true);

        // Check new ordering
        assert_eq!(
            b.nodes(),
            vec![n4a.clone(), n3.clone(), n2.clone(), n5.clone()]
        );
    }
}
