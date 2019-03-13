/**
 * rust-kad
 * Generic NodeTable definition
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use std::ops::Range;

use crate::id::DatabaseId;
use crate::node::Node;

// Generic Node Table implementation
// This keeps track of known nodes
pub trait NodeTable<Id: DatabaseId + Clone + 'static, Node: Clone + 'static> {
    /// Register or update node data
    /// This is called to attempt to add or update a node in the node table
    /// Returns true if node has been stored or updated, false if there is no room remaining in the table
    fn register(&mut self, id: &Id, node: Node) -> bool;

    /// Update node metadata
    /// This is called when messages are received / lost / error to keep track of node connectivity
    fn update(&mut self, id: &Id) -> bool;

    // Find nearest nodes
    // Returns a list of the nearest nodes to the provided id
    fn nearest(&mut self, id: &Id, range: Range<usize>) -> Vec<(Id, Node)>;
    
    // Find an exact node
    // This is used to fetch a node from the node table
    fn contains(&self, id: &Id) -> Option<Node>;

    // Peek at the oldest node from the k-bucket containing it
    // This returns an instance of the oldest node while leaving it in the appropriate bucket
    fn peek_oldest(&mut self, id: &Id) -> Option<(Id, Node)>;

    // Replace a node in a given bucket
    // This is used to replace an expired node in the bucket
    fn replace(&mut self, id: Id, old: Node, new: Node);
}

