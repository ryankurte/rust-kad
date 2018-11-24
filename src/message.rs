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
    Store(ID, VALUE),
    FindNode(ID),
    FindValue(ID),
}

#[derive(PartialEq, Clone, Debug)]
pub enum Response<ID, ADDR, VALUE> {
    NodesFound(Vec<Node<ID, ADDR>>),
    ValuesFound(Vec<VALUE>),
    NoResult,
}

#[derive(PartialEq, Clone, Debug)]
pub struct RequestMessage<ID, ADDR> {
    pub request_id: ID,
    pub caller: Node<ID, ADDR>,
    pub request: Request<ID, ADDR>,
}

impl <ID, ADDR> RequestMessage<ID, ADDR> {
    pub fn ping(request_id: ID, caller: Node<ID, ADDR>) -> RequestMessage<ID, ADDR> {
        RequestMessage{request_id, caller, request: Request::Ping}
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
    Request(RequestMessage<ID, ADDR>),
    Response(ResponseMessage<ID, ADDR, VALUE>),
}

