/**
 * rust-kad
 * Kademlia message implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


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
pub enum Response<Id, Node, Value> {
    // NodesFound contains the search address and a list of near nodes
    NodesFound(Id, Vec<(Id, Node)>),
    /// ValuesFound contains the search address and a list of near values
    ValuesFound(Id, Vec<Value>),
    /// NoResult indicates no nodes or values were located
    NoResult,
}


#[derive(PartialEq, Clone, Debug)]
pub struct RequestMessage<Id, Node, Value> {
    pub request_id: Id,
    pub caller: Node,
    pub request: Request<Id, Value>,
}

impl <Id, Node, Value> RequestMessage<Id, Node, Value> {
    pub fn ping(request_id: Id, caller: Node) -> RequestMessage<Id, Node, Value> {
        RequestMessage{request_id, caller, request: Request::Ping}
    }

    pub fn find_node(request_id: Id, caller: Node, id: Id) -> RequestMessage<Id, Node,Value> {
        RequestMessage{request_id, caller, request: Request::FindNode(id)}
    }

    pub fn find_values(request_id: Id, caller: Node, id: Id) -> RequestMessage<Id, Node,Value> {
        RequestMessage{request_id, caller, request: Request::FindValue(id)}
    }

    pub fn store(request_id: Id, caller: Node, id: Id, value: Vec<Value>) -> RequestMessage<Id, Node, Value> {
        RequestMessage{request_id, caller, request: Request::Store(id, value)}
    }
}


#[derive(PartialEq, Clone, Debug)]
pub struct ResponseMessage<Id, Node, Value> {
    pub request_id: Id,
    pub responder: Node,
    pub response: Response<Id, Node, Value>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum Message<Id, Node, Value> {
    Request(RequestMessage<Id, Node, Value>),
    Response(ResponseMessage<Id, Node, Value>),
}

