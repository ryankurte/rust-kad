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

impl<Id: std::fmt::Debug, Value> std::fmt::Display for Request<Id, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ping => write!(f, "Ping"),
            Self::FindNode(id) => write!(f, "FindNode({:?})", id),
            Self::FindValue(id) => write!(f, "FindValues({:?})", id),
            Self::Store(id, values) => write!(f, "Store({:?}, {} values)", id, values.len()),
        }
    }
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

impl<Id: std::fmt::Debug, Info, Value> std::fmt::Display for Response<Id, Info, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodesFound(id, entries) => {
                write!(f, "NodesFound({:?}, {} nodes)", id, entries.len())
            }
            Self::ValuesFound(id, values) => {
                write!(f, "ValuesFound({:?}, {} values)", id, values.len())
            }
            Self::NoResult => write!(f, "NoResult"),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct RequestMessage<Id, Info, Value, ReqId> {
    pub req_id: ReqId,
    pub caller: Entry<Id, Info>,
    pub request: Request<Id, Value>,
}

impl<Id, Info, Value, ReqId> RequestMessage<Id, Info, Value, ReqId> {
    pub fn ping(req_id: ReqId, caller: Entry<Id, Info>) -> RequestMessage<Id, Info, Value, ReqId> {
        RequestMessage {
            req_id,
            caller,
            request: Request::Ping,
        }
    }

    pub fn find_node(
        req_id: ReqId,
        caller: Entry<Id, Info>,
        id: Id,
    ) -> RequestMessage<Id, Info, Value, ReqId> {
        RequestMessage {
            req_id,
            caller,
            request: Request::FindNode(id),
        }
    }

    pub fn find_values(
        req_id: ReqId,
        caller: Entry<Id, Info>,
        id: Id,
    ) -> RequestMessage<Id, Info, Value, ReqId> {
        RequestMessage {
            req_id,
            caller,
            request: Request::FindValue(id),
        }
    }

    pub fn store(
        req_id: ReqId,
        caller: Entry<Id, Info>,
        id: Id,
        value: Vec<Value>,
    ) -> RequestMessage<Id, Info, Value, ReqId> {
        RequestMessage {
            req_id,
            caller,
            request: Request::Store(id, value),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct ResponseMessage<Id, Info, Value, ReqId> {
    pub req_id: ReqId,
    pub responder: Entry<Id, Info>,
    pub response: Response<Id, Info, Value>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum Message<Id, Info, Value, ReqId> {
    Request(RequestMessage<Id, Info, Value, ReqId>),
    Response(ResponseMessage<Id, Info, Value, ReqId>),
}
