/**
 * rust-kad
 * Kademlia base operation, used in the implementation of connect / locate / search / store functions
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::fmt::{Debug, Display};
use std::time::Instant;
use std::collections::HashMap;

use futures::channel::mpsc::{Sender};

use log::{trace, debug, info, warn};

use crate::Config;

use crate::common::*;


/// RequestState is used to store the state of pending requests
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RequestState {
    Pending,
    Active,
    Timeout,
    Complete,
    InvalidResponse,
}

#[derive(Debug)]
pub enum OperationKind<Id, Info, Data> {
    /// Find a node with the specified ID
    FindNode(Sender<Result<Entry<Id, Info>, Error>>),
    /// Find values at the specified ID
    FindValues(Sender<Result<Vec<Data>, Error>>),
    /// Store the provided value(s) at the specified ID
    Store(Vec<Data>, Sender<Result<Vec<Entry<Id, Info>>, Error>>),
}

impl <Id, Info, Data> Display for OperationKind<Id, Info, Data> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationKind::FindNode(_) => write!(f, "FindNode"),
            OperationKind::FindValues(_) => write!(f, "FindValues"),
            OperationKind::Store(_, _) => write!(f, "Store"),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum OperationState {
    Init,
    Search(usize),
    Searching(usize),
    Request,
    Pending,
    Done,
}


#[derive(Debug)]
pub(crate) struct Operation<Id, Info, Data, ReqId> {
    pub req_id: ReqId,
    pub target: Id,
    pub kind: OperationKind<Id, Info, Data>,

    pub state: OperationState,
    pub last_update: Instant,
    pub nodes: HashMap<Id, (Entry<Id, Info>, RequestState)>,
    pub data: HashMap<Id, Vec<Data>>,
}

impl <Id, Info, Data, ReqId> Operation<Id, Info, Data, ReqId> {
    pub(crate) fn new(req_id: ReqId, target: Id, kind: OperationKind<Id, Info, Data>) -> Self {
        Self {
            req_id,
            target,
            kind,
            state: OperationState::Init,
            last_update: Instant::now(),
            nodes: HashMap::new(),
            data: HashMap::new(),
        }
    }



}

