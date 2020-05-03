/**
 * rust-kad
 * Kademlia message implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use super::entry::Entry;

#[derive(PartialEq, Clone, Debug)]
pub enum Request<Id, Value> {
    Ping,
    FindNode(Id),
    FindValue(Id),
    Store(Id, Vec<Value>),
}

impl<Id, Value> Request<Id, Value> {
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
pub enum Response<Id, Info, Value> {
    NodesFound(Id, Vec<Entry<Id, Info>>),
    ValuesFound(Id, Vec<Value>),
    NoResult,
}

#[derive(PartialEq, Clone, Debug)]
pub struct RequestMessage<Id, Info, Value> {
    pub request_id: Id,
    pub caller: Entry<Id, Info>,
    pub request: Request<Id, Value>,
}

impl<Id, Info, Value> RequestMessage<Id, Info, Value> {
    pub fn ping(request_id: Id, caller: Entry<Id, Info>) -> RequestMessage<Id, Info, Value> {
        RequestMessage {
            request_id,
            caller,
            request: Request::Ping,
        }
    }

    pub fn find_node(
        request_id: Id,
        caller: Entry<Id, Info>,
        id: Id,
    ) -> RequestMessage<Id, Info, Value> {
        RequestMessage {
            request_id,
            caller,
            request: Request::FindNode(id),
        }
    }

    pub fn find_values(
        request_id: Id,
        caller: Entry<Id, Info>,
        id: Id,
    ) -> RequestMessage<Id, Info, Value> {
        RequestMessage {
            request_id,
            caller,
            request: Request::FindValue(id),
        }
    }

    pub fn store(
        request_id: Id,
        caller: Entry<Id, Info>,
        id: Id,
        value: Vec<Value>,
    ) -> RequestMessage<Id, Info, Value> {
        RequestMessage {
            request_id,
            caller,
            request: Request::Store(id, value),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct ResponseMessage<Id, Info, Value> {
    pub request_id: Id,
    pub responder: Entry<Id, Info>,
    pub response: Response<Id, Info, Value>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum Message<Id, Info, Value> {
    Request(RequestMessage<Id, Info, Value>),
    Response(ResponseMessage<Id, Info, Value>),
}
