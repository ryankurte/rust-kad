/**
 * rust-kad
 * Kademlia node type
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::time::{Instant};
use std::fmt::Debug;

use crate::id::DatabaseId;

#[derive(Clone, Debug, Hash, Eq)]
pub struct Node<Id, Addr> {
    id: Id,
    address: Addr,
    seen: Option<Instant>,
    frozen: bool,
}

impl <Id, Addr> PartialEq for Node<Id, Addr> 
where
    Id: PartialEq,
    Addr: PartialEq,
{
    fn eq(&self, other: &Node<Id, Addr>) -> bool {
        self.id == other.id && self.address == other.address
    }
}

impl <Id, Addr>Node<Id, Addr> 
where 
    Id: DatabaseId + Clone + Debug + 'static,
    Addr: Clone + Debug + 'static,
{
    pub fn new(id: Id, address: Addr) -> Node<Id, Addr> {
        Node{id, address, seen: None, frozen: false}
    }

    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn address(&self) -> &Addr {
        &self.address
    }

    pub fn set_address(&mut self, address: &Addr) {
        self.address = address.clone();
    }

    pub fn seen(&self) -> Option<Instant> {
        self.seen
    }

    pub fn set_seen(&mut self, seen: Instant) {
        self.seen = Some(seen);
    }
}

impl <Id, Addr> From<(Id, Addr)> for Node<Id, Addr> 
where 
    Id: DatabaseId + Clone + Debug + 'static,
    Addr: Clone + Debug + 'static,
{
    fn from(d: (Id, Addr)) -> Node<Id, Addr> {
        Node::new(d.0, d.1)
    }
}

impl <Id, Addr> Into<(Id, Addr)> for Node<Id, Addr> 
where 
    Id: DatabaseId + Clone + Debug + 'static,
    Addr: Clone + Debug + 'static,
{
    fn into(self) -> (Id, Addr) {
        (self.id, self.address)
    }
}
