
use crate::id::DatabaseId;
use crate::node::Node;

// Generic Node Table implementation
// This keeps track of known nodes
pub trait NodeTable<ID: DatabaseId + Clone + 'static, ADDR: Clone + 'static> {
    // Update a node in the table
    // Returns true if node has been stored or updated, false if there is no room remaining in the table
    fn update(&mut self, node: &Node<ID, ADDR>) -> bool;
    // Find nearest nodes
    // Returns a list of the nearest nodes to the provided id
    fn nearest(&mut self, id: &ID, count: usize) -> Vec<Node<ID, ADDR>>;
    // Find an exact node
    // This is used to fetch a node from the node table
    fn contains(&self, id: &ID) -> Option<Node<ID, ADDR>>;

    // Peek at the oldest node from the k-bucket containing it
    // This returns an instance of the oldest node while leaving it in the appropriate bucket
    fn peek_oldest(&mut self, id: &ID) -> Option<Node<ID, ADDR>>;
    // Replace a node in a given bucket
    // This is used to replace an expired node in the bucket
    fn replace(&mut self, node: &Node<ID, ADDR>, replacement: &Node<ID, ADDR>);
}

