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
    // Update a node in the table
    // Returns true if node has been stored or updated, false if there is no room remaining in the table
    fn update(&mut self, node: &Entry<Id, Info>) -> bool;
    // Find nearest nodes
    // Returns a list of the nearest nodes to the provided id
    fn nearest(&mut self, id: &Id, range: Range<usize>) -> Vec<Entry<Id, Info>>;
    // Find an exact node
    // This is used to fetch a node from the node table
    fn contains(&self, id: &Id) -> Option<Entry<Id, Info>>;

    // Peek at the oldest node from the k-bucket containing it
    // This returns an instance of the oldest node while leaving it in the appropriate bucket
    fn peek_oldest(&mut self, id: &Id) -> Option<Entry<Id, Info>>;
    // Replace a node in a given bucket
    // This is used to replace an expired node in the bucket
    fn replace(&mut self, node: &Entry<Id, Info>, replacement: &Entry<Id, Info>);
}
