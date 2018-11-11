
use crate::node::Node;

pub enum UpdateResult<ID, ADDR> {
    Ok,
    Pending(Node<ID, ADDR>),
}

// Generic KBucket implementation
// This keeps track of known nodes
pub trait KBucket<ID, ADDR> {
    // Update a node in the table
    fn update(&mut self, node: &Node<ID, ADDR>) -> bool;
    // Find nearest nodes
    fn find_nearest(&mut self, id: &ID) -> Vec<Node<ID, ADDR>>;
    // Peek at the oldest node from the k-bucket containing it
    fn peek_oldest(&mut self, id: &ID) -> Option<Node<ID, ADDR>>;
    // Replace a node
    fn replace(&mut self, node: &Node<ID, ADDR>, replacement: &Node<ID, ADDR>);
}

