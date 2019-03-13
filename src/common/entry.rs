/**
 * rust-kad
 * Kademlia node type
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::time::{Instant};
use std::fmt::Debug;

use super::id::DatabaseId;

#[derive(Clone, Debug, Hash, Eq)]
pub struct Entry<Id, Info> {
    id: Id,
    info: Info,
    seen: Option<Instant>,
    frozen: bool,
}

impl <Id, Info> PartialEq for Entry<Id, Info> 
where
    Id: PartialEq,
    Info: PartialEq,
{
    fn eq(&self, other: &Entry<Id, Info>) -> bool {
        self.id == other.id && self.info == other.info
    }
}

impl <Id, Info>Entry<Id, Info> 
where 
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
{
    pub fn new(id: Id, info: Info) -> Entry<Id, Info> {
        Entry{id, info, seen: None, frozen: false}
    }

    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn info(&self) -> &Info {
        &self.info
    }

    pub fn set_info(&mut self, info: &Info) {
        self.info = info.clone();
    }

    pub fn seen(&self) -> Option<Instant> {
        self.seen
    }

    pub fn set_seen(&mut self, seen: Instant) {
        self.seen = Some(seen);
    }
}

impl <Id, Info> From<(Id, Info)> for Entry<Id, Info> 
where 
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
{
    fn from(d: (Id, Info)) -> Entry<Id, Info> {
        Entry::new(d.0, d.1)
    }
}

impl <Id, Info> Into<(Id, Info)> for Entry<Id, Info> 
where 
    Id: DatabaseId + Clone + Debug + 'static,
    Info: Clone + Debug + 'static,
{
    fn into(self) -> (Id, Info) {
        (self.id, self.info)
    }
}
