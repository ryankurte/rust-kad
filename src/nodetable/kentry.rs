
use std::fmt::Debug;

#[derive(Clone, Debug, PartialEq)]
pub struct KEntry<Id, Node> {
    id: Id,
    node: Node,
}

impl <Id, Node> KEntry<Id, Node> {
    pub fn new(id: Id, node: Node) -> Self {
        KEntry{id, node}
    }

    pub fn id(&self) -> Id {
        self.id
    }
    
    pub fn node(&self) -> Node {
        self.node
    }
}
