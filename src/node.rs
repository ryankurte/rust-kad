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

#[derive(Clone, Debug)]
pub struct Node<ID, ADDR> {
    id: ID,
    address: ADDR,
    seen: Option<Instant>,
    frozen: bool,
}

impl <ID, ADDR> PartialEq for Node<ID, ADDR> 
where
    ID: PartialEq,
    ADDR: PartialEq,
{
    fn eq(&self, other: &Node<ID, ADDR>) -> bool {
        self.id == other.id && self.address == other.address
    }
}

impl <ID, ADDR>Node<ID, ADDR> 
where 
    ID: DatabaseId + Clone + Debug + 'static,
    ADDR: Clone + Debug + 'static,
{
    pub fn new(id: ID, address: ADDR) -> Node<ID, ADDR> {
        Node{id, address, seen: None, frozen: false}
    }

    pub fn id(&self) -> &ID {
        &self.id
    }

    pub fn address(&self) -> &ADDR {
        &self.address
    }

    pub fn set_address(&mut self, address: &ADDR) {
        self.address = address.clone();
    }

    pub fn seen(&self) -> Option<Instant> {
        self.seen
    }

    pub fn set_seen(&mut self, seen: Instant) {
        self.seen = Some(seen);
    }
}

