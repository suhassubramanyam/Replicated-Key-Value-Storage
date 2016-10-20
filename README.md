# Replicated-Key-Value-Storage
Implementation of a Dynamo-style key-value storage

This is the working code of an Android app for CSE586-Distributed Systems course offered in spring 2016 at UB.

Requirements include :

-Support for insert/query/delete operations.

-There are always 5 nodes in the system. There is no need to implement adding/removing nodes from the system.However, there can be at most 1 node failure at any given time.

-All failures are temporary; you can assume that a failed node will recover soon, i.e., it will not be permanently unavailable during a run.

-When a node recovers, it should copy all the object writes it missed during the failure. This can be done by asking the right nodes and copy from them.

-Content provider should support concurrent read/write operations.

-Content provider should handle a failure happening at the same time with read/write operations.

-Replication should be done exactly the same way as Dynamo does. In other words, a (key, value) pair should be replicated over three consecutive partitions, starting from the partition that the key belongs to.

-All replicas should store the same value for each key. This is “per-key” consistency. There is no consistency guarantee you need to provide across keys. More formally, you need to implement per-key linearizability.

Criteria for testing are :

Testing basic ops

This phase will test basic operations, i.e., insert, query, delete, @, and *. This will test if everything is correctly replicated. There is no concurrency in operations and there is no failure either.

Testing concurrent ops with different keys

This phase will test if your implementation can handle concurrent operations under no failure. The tester will use independent (key, value) pairs inserted/queried concurrently on all the nodes.

Testing concurrent ops with same keys

This phase will test if your implementation can handle concurrent operations with same keys under no failure. The tester will use the same set of (key, value) pairs inserted/queried concurrently on all the nodes.

Testing one failure

This phase will test one failure with every operation. One node will crash before operations start. After all the operations are done, the node will recover. This will be repeated for each and every operation.

Testing concurrent operations with one failure

This phase will execute operations concurrently and crash one node in the middle of the execution. After some time, the failed node will also recover in the middle of the execution.

Testing concurrent operations with one consistent failure

This phase will crash one node at a time consistently, i.e., one node will crash then recover, and another node will crash and recover, etc. There will be a brief period of time in between the crash-recover sequence.
