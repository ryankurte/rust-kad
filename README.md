# rust-kad

A generic / futures based implementation of the Kademlia DHT, heavily inspired by [dtantsur/rust-dht](https://github.com/dtantsur/rust-dht) with the goal of providing a simple to use, well-tested, low-dependency, futures-based API for using DHTs with arbitrary communication mechanisms and encodings

## Usage

### Configuration

- k - bucket size / system wide replication parameter
- alpha - system wide concurrency parameter

## Status

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

- [ ] Search - common to most operations
  - [ ] Select alpha closest nodes
  - [ ] Send RPCs to selected subset of nodes
  - [ ] If no suitable responses, expand to k closest nodes
  - [ ] Recurse until responses received from k closest nodes

- [ ] Find Node
  - [ ] Search using FIND_NODE RPC

- [ ] Find Value
  - [ ] Search using FIND_VALUE RPC
  - [ ] Once values returned, store (k, v) pair at the closest node observed that did not return the value

- [ ] Store Value
  - [ ] Search using FIND_NODE RPC
  - [ ] Send STORE RPC to k closest nodes

- [ ] Join
  - [ ] Insert known node into appropriate k-bucket
  - [ ] Perform Find Node lookup on own ID
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

### Questions

- How is FindValues usually handled where there can be more than one value per ID?
- Is there a case when STORE is valid when the origin ID is closer to the requester ID than the local ID?
  - This seems like it could be ignored


