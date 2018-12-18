# rust-kad

A generic / futures based implementation of the Kademlia DHT, heavily inspired by [dtantsur/rust-dht](https://github.com/dtantsur/rust-dht) with the goal of providing a simple to use, well-tested, low-dependency, futures-based API for using DHTs with arbitrary communication mechanisms and encodings

## Usage

### Configuration

- k - system wide replication parameter, defines bucket sizes and search breadth
- concurrency - system wide concurrency parameter, number of parallel operations to run at once

## Status

[![GitHub tag](https://img.shields.io/github/tag/ryankurte/rust-kad.svg)](https://github.com/ryankurte/rust-kad)
[![Build Status](https://travis-ci.com/ryankurte/rust-kad.svg?branch=master)](https://travis-ci.com/ryankurte/rust-kad)
[![Crates.io](https://img.shields.io/crates/v/kad.svg)](https://crates.io/crates/kad)
[![Docs.rs](https://docs.rs/kad/badge.svg)](https://docs.rs/kad)

[Open Issues](https://github.com/ryankurte/rust-kad/issues)


***Work In Progress***

### Components

- [ ] Receive message
  - [x] Update appropriate k-bucket
    - [x] Add node if bucket not full
    - [ ] Store pending if bucket full and ping oldest (if > seen time)
  - [x] Respond to Ping with NoResult
  - [x] Respond to FindNodes with NodesFound
  - [x] Respond to FindValues with NodesFound or ValuesFound
  - [ ] For new node, Send STORE RPC if own ID is closer to key than known nearby nodes

- [x] Search - common to most operations
  - [x] Select alpha closest nodes
  - [x] Send RPCs to selected subset of nodes
  - [ ] If no suitable responses, expand to k closest nodes
  - [x] Recurse until responses received from k closest nodes

- [x] Find Node
  - [x] Search using FIND_NODE RPC
  - [x] Return node

- [ ] Find Value
  - [x] Search using FIND_VALUE RPC
  - [x] Collect values
  - [ ] Once values returned, store (k, v) pair at the closest node observed that did not return the value

- [x] Store Value
  - [x] Search using FIND_NODE RPC
  - [x] Send STORE RPC to k closest nodes

- [ ] Connect
  - [x] Insert known node into appropriate k-bucket
  - [x] Perform Find Node lookup on own ID
  - [ ] Refresh all k-buckets further than the closest neighbor

- [ ] Maintanence
  - [ ] Remove non-responsive / old contacts
  - [ ] Expire values
    - [ ] Basic expiry after defined time
    - [ ] Cache expiry exponentially inversely proportional to number of nodes between current node and closest to key ID node
  - [ ] Refresh k-buckets
    - [ ] FIND_NODES to random ID in any bucket not queried in a configurable period
  - [ ] Re-publish values
    - [ ] STORE RPC to K nodes at defined period (hourly)
    - [ ] Unless a STORE RPC has been received in the same period

- [ ] Buckets
  - [ ] Implement bucket splitting (if it can be done more efficiently than existing?)
    - Useful for maintanence / don't need to message unused buckets
  - [ ] Reverse / generate random IDs in bucket for maintanence purposes

### Questions

- How is FindValues usually handled where there can be more than one value per ID?
- Is there a case when STORE is valid when the origin ID is closer to the requester ID than the local ID?
  - This seems like it could be ignored


