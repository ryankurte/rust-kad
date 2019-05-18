/**
 * rust-kad
 * Generic NodeTable definition
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::ops::Range;

use crate::common::{DatabaseId, Entry};

// Generic Node Table implementation
// This keeps track of known nodes
pub trait NodeTable<Id: DatabaseId + Clone + 'static, Info: Clone + 'static> {
    /// Create or update a node in the table
    /// Returns true if node has been stored or updated, false if there is no room remaining in the table
    fn create_or_update(&mut self, node: &Entry<Id, Info>) -> bool;

    /// Find nearest nodes
    /// Returns a list of the nearest nodes to the provided id
    fn nearest(&mut self, id: &Id, range: Range<usize>) -> Vec<Entry<Id, Info>>;

    /// Find an exact node
    /// This is used to fetch a node from the node table
    fn contains(&self, id: &Id) -> Option<Entry<Id, Info>>;

    /// Iterate through the oldest nodes in each bucket
    fn iter_oldest(&mut self) -> Box<Iterator<Item=Entry<Id, Info>>>;

    /// Update an entry in the bucket by ID
    /// Returns true if update function has been executed, false if no entry was found
    fn update_entry<F>(&mut self, id: &Id, f: F) -> bool
    where F: Fn(&mut Entry<Id, Info>);

    /// Remove an entry in the bucket by ID
    fn remove_entry(&mut self, id: &Id);
}

