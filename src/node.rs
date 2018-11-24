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

#[derive(PartialEq, Clone, Debug)]
pub struct Node<ID, ADDR> {
    id: ID,
    address: ADDR,
    seen: Option<Instant>,
    frozen: bool,
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
}

