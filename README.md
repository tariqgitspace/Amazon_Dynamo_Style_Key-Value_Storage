# Amazon_Dynamo_Style_Key-Value_Storage
distributed Hash Table based on Chord for a group of 5 Android Devices for purpose of partitioning, replication and failure handling. This Degree 3 quorum implementation provided availability and linearizability at the same time. In other words, the implementation will always perform read and write operations successfully even under failures of atmost 2 nodes. At the same time, a read operation (linearizability)always returns the most recent value. 


There are three main pieces that have been implemented:

1. Partitioning : sha1: for load balancing
2. Replication : availability and linearizability
3. Failure handling: Node fails and recovers back


a> Membership :
Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without using a ring-based routing.

b> Request routing :
Request comes to any arbotraray node initally 
Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node. Under no failures, all requests are directly forwarded to the coordinator, and the coordinator should be in charge of serving read/write operations.

c> Chain replication :
//linearizability
This replication strategy provides linearizability. In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write. A read operation always comes to the last partition and reads the value from the last partition.

d> Failure handling :
Just as the original Dynamo, each request is used to detect a node failure. For this purpose, a timeout for a socket read is used; and if a node does not respond within the timeout, it is considered as a failed node. When a coordinator for a request fails and it does not respond to the request, its successor is contacted next for the request.


Just like Simple DHT, implementation supports and implements all storage functionalities i.e. Create server and client threads, open sockets, and respond to incoming requests. When a node recovers, it copies all the object writes it missed during the failure. This is done by asking the right nodes and then copy from them.

For write operations, all objects are versioned in order to distinguish stale copies from the most recent copy. For read operations, if the readers in the reader quorum have different versions of the same object from different nodes, the coordinator picks the most recent version and returns it.



