/**
 * rust-kad
 * Kademlia message implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */

use crate::node::Node;

#[derive(PartialEq, Clone, Debug)]
pub enum Request<ID, VALUE> {
    Ping,
    FindNode(ID),
    FindValue(ID),
    Store(ID, Vec<VALUE>),
}

#[derive(PartialEq, Clone, Debug)]
pub enum Response<ID, ADDR, VALUE> {
    NodesFound(Vec<Node<ID, ADDR>>),
    ValuesFound(Vec<VALUE>),
    NoResult,
}

#[derive(PartialEq, Clone, Debug)]
pub struct RequestMessage<ID, ADDR, VALUE> {
    pub request_id: ID,
    pub caller: Node<ID, ADDR>,
    pub request: Request<ID, VALUE>,
}

impl <ID, ADDR, VALUE> RequestMessage<ID, ADDR, VALUE> {
    pub fn ping(request_id: ID, caller: Node<ID, ADDR>) -> RequestMessage<ID, ADDR, VALUE> {
        RequestMessage{request_id, caller, request: Request::Ping}
    }

    pub fn find_node(request_id: ID, caller: Node<ID, ADDR>, id: ID) -> RequestMessage<ID, ADDR, VALUE> {
        RequestMessage{request_id, caller, request: Request::FindNode(id)}
    }

    pub fn find_values(request_id: ID, caller: Node<ID, ADDR>, id: ID) -> RequestMessage<ID, ADDR, VALUE> {
        RequestMessage{request_id, caller, request: Request::FindValue(id)}
    }

    pub fn store(request_id: ID, caller: Node<ID, ADDR>, id: ID, value: Vec<VALUE>) -> RequestMessage<ID, ADDR, VALUE> {
        RequestMessage{request_id, caller, request: Request::Store(id, value)}
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct ResponseMessage<ID, ADDR, VALUE> {
    pub request_id: ID,
    pub responder: Node<ID, ADDR>,
    pub response: Response<ID, ADDR, VALUE>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum Message<ID, ADDR, VALUE> {
    Request(RequestMessage<ID, ADDR, VALUE>),
    Response(ResponseMessage<ID, ADDR, VALUE>),
}

