
use std::fmt::Debug;
use std::time::{Instant};

#[derive(Clone, Debug, PartialEq)]
pub struct KEntry<Id, Node> {
    id: Id,
    node: Node,
    seen: Instant
}

impl <Id, Node> KEntry<Id, Node> {
    pub fn new(id: Id, node: Node) -> Self {
        KEntry{id, node, seen: Instant::now()}
    }

    pub fn id(&self) -> &Id {
        &self.id
    }
    
    pub fn node(&self) -> &Node {
        &self.node
    }

    pub fn seen(&self) -> Instant {
        self.seen
    }

    fn set_seen(&mut self, when: Instant) {
        self.seen = when;
    }
}
