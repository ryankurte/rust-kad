/**
 * rust-kad
 * Kademlia message implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use crate::node::Node;

#[derive(PartialEq, Clone, Debug)]
pub enum Request<Id, Value> {
    Ping,
    FindNode(Id),
    FindValue(Id),
    Store(Id, Vec<Value>),
}


impl <Id, Value> Request<Id, Value> {

    /// Fetch the database ID for the request (if present)
    pub fn id(&self) -> Option<&Id> {
        match self {
            Request::Ping => None,
            Request::FindNode(id) => Some(id),
            Request::FindValue(id) => Some(id),
            Request::Store(id, _) => Some(id),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub enum Response<Id, Addr, Value> {
    NodesFound(Id, Vec<Node<Id, Addr>>),
    ValuesFound(Id, Vec<Value>),
    NoResult,
}


#[derive(PartialEq, Clone, Debug)]
pub struct RequestMessage<Id, Addr, Value> {
    pub request_id: Id,
    pub caller: Node<Id, Addr>,
    pub request: Request<Id, Value>,
}

impl <Id, Addr,Value> RequestMessage<Id, Addr,Value> {
    pub fn ping(request_id: Id, caller: Node<Id, Addr>) -> RequestMessage<Id, Addr,Value> {
        RequestMessage{request_id, caller, request: Request::Ping}
    }

    pub fn find_node(request_id: Id, caller: Node<Id, Addr>, id: Id) -> RequestMessage<Id, Addr,Value> {
        RequestMessage{request_id, caller, request: Request::FindNode(id)}
    }

    pub fn find_values(request_id: Id, caller: Node<Id, Addr>, id: Id) -> RequestMessage<Id, Addr,Value> {
        RequestMessage{request_id, caller, request: Request::FindValue(id)}
    }

    pub fn store(request_id: Id, caller: Node<Id, Addr>, id: Id, value: Vec<Value>) -> RequestMessage<Id, Addr,Value> {
        RequestMessage{request_id, caller, request: Request::Store(id, value)}
    }
}


#[derive(PartialEq, Clone, Debug)]
pub struct ResponseMessage<Id, Addr, Value> {
    pub request_id: Id,
    pub responder: Node<Id, Addr>,
    pub response: Response<Id, Addr,Value>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum Message<Id, Addr, Value> {
    Request(RequestMessage<Id, Addr, Value>),
    Response(ResponseMessage<Id, Addr, Value>),
}

