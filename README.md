# raft-kv

A replicated, fault-tolerant key-value store written in Go, powered by a custom, from-scratch implementation of the Raft consensus algorithm.

## Technical Overview

### Consensus Core (`raft/`)

- **Leader Election:** Implements randomized election timeouts and strict election restrictions (nodes vote only for candidates with logs at least as up-to-date as their own).
- **Log Replication:** Handles state replication using AppendEntries consistency checks, automatic conflict truncation, and leader-side commit advancement (leaders commit entries from prior terms only by committing an entry from their current term).
- **No-Op Initialization:** Newly elected leaders automatically append a no-op entry to commit pending entries from previous terms immediately.
- **Sequential Apply Channel:** Driven by a single applier goroutine, ensuring committed state machine operations are delivered in strict sequential order.
- **Fault Injection Framework:** Supports dynamic network and node fault simulations, including permanent node crashes (`Kill`) and network isolation/restoration (`Disconnect` / `Reconnect`).

### Key-Value State Machine (`kv/`)

- **KVServer:** Wraps a Raft node to execute commands. `Put` and `Delete` requests pass through the Raft log and return only after being committed and applied. If a leadership change overwrites a pending proposal, the operation explicitly fails rather than returning false success.
- **Get Operations:** Served locally by whichever node currently believes itself to be the cluster leader.
- **Clerk Client:** A stateless client wrapper that tracks leader hints, automatically rotates through cluster nodes upon failure, and retries operations until reaching a configurable deadline.

## Testing & Tooling

- **Automated Cluster Test Suite (`kv/cluster_test.go`):** Exercises leader elections, log replication, deletions, write-ordering constraints, leader failovers, uncommittable partitioned leaders, recovery after loss of quorum, and heavy client concurrency.
- **Interactive Shell (`main.go`):** Running `go run main.go` launches an interactive REPL for issuing key-value operations and manually injecting node crashes or network partitions in real time.

## Motivation & Architecture

This project was built to gain a deep, hands-on understanding of distributed consensus through implementation rather than theory alone. While the Raft paper is remarkably clear, the primary engineering difficulties emerge during execution:

- Managing concurrent state mutations across asynchronous goroutines
- Handling stale RPC responses gracefully
- Enforcing precise log-commit boundaries across term changes
- Handling pending writes mid-flight when leadership shifts

Go was selected because goroutines and channels align naturally with Raft's concurrent state machines, and the MIT 6.824 curriculum provided a strong benchmark for correctness. The system was constructed incrementally: starting with basic leader election, expanding to log replication, wiring up the apply pipeline, building the key-value state machine, and finally adding the client interaction layer.

## Known Limitations & Trade-offs

- **In-Memory State (No Persistence):** State (`currentTerm`, `votedFor`, and the log) is stored strictly in volatile memory. A crashed node cannot recover its prior state; calls to `Kill` are permanent.
- **Non-Linearizable Reads:** `Get` requests are answered locally by the perceived leader without contacting a quorum. A leader isolated by a network partition can return stale data until it discovers its replacement. *(Resolution: implement ReadIndex or route read operations through the consensus log.)*
- **At-Least-Once Request Delivery:** Retried `Put` operations following a client timeout can be applied twice. While safe for idempotent commands (`SET` and `DEL`), non-idempotent operations require unique client IDs and monotonic sequence numbers.
- **Unbounded Log Growth:** Log compaction and state snapshotting are omitted, meaning the log grows indefinitely during operation.
- **Static Cluster Membership:** Node identities and cluster size are fixed at startup; dynamic reconfigurations are unsupported.
- **No Pre-Vote Phase:** Partitioned followers continuously increment their `currentTerm`, causing unnecessary election disruptions when rejoining the cluster.
- **In-Process Network Simulation:** Nodes communicate via direct Go method calls within a single process. Tests cover node crashes and partitions, but do not model physical network properties like packet loss, variable delays, or reordering over TCP.
