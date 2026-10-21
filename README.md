# Chord Distributed Hash Table Simulator

**An F# / Akka.NET simulation of the Chord peer-to-peer lookup protocol: peers with 160-bit SHA-1 identifiers join a ring, maintain successor pointers and finger tables, and the program reports the average number of hops a key lookup takes.**

## Overview

Chord (Stoica et al., 2001) places nodes on an identifier ring and routes a lookup for key `k` to the first node whose ID succeeds `k`, using a finger table so that each hop roughly halves the remaining distance. This project implements that protocol as Akka.NET actors in a single F# file: every peer is an actor named after its numeric ID, new peers join through the first peer, and two background loops drive Chord's `stabilize` and `fix_fingers` maintenance every 100 ms. After all peers have been told to join, every peer issues `numRequests` lookups for random keys; a `Simulator` actor sums the hop counts and prints the average. It targets .NET 7 with Akka 1.4.25 and was written as a team programming assignment ("PA2") for COP 5615, Distributed Operating System Principles, at the University of Florida in fall 2023 — the same course and team as the companion `concurrent-client-server` and `distributed-computing-model` repositories.

## What it does

- Takes two command-line arguments, `<numNodes> <numRequests>`, and spawns one `peer` actor per node plus one `Simulator` actor.
- Generates 160-bit identifiers (`numberGenerator.nRandomForId`): SHA-1 of 20 random bytes, converted to a `bigint` by `getDecimalValues`. The bit length is computed as `(2^4) * 10 = 160`, so the ring has `2^160` positions.
- Joins peers one at a time: the first peer is its own successor and predecessor; each later peer sends `Join firstPeer`, which resolves its successor through `FindSuccessor`.
- Runs Chord maintenance continuously from two `async` loops: `Maintain` (stabilize — ask your successor for its predecessor and adopt it if it lies between you) and `FingersToBeFixed` (fix one finger entry per tick, cycling through all 160).
- Resolves `numRequests` random keys from every peer, counting hops per lookup, and prints `The average hop count for <N> peers and <R> requests is <x>`.

## How it works

| File | Role |
| --- | --- |
| `PA2/Program.fs` | Everything: message types, `numberGenerator`, the `simulator` and `peer` actors, and the top-level script that spawns peers, starts the maintenance loops and fires the lookups. |
| `PA2/PA2.fsproj` | .NET 7 console project referencing `Akka`, `Akka.FSharp`, `Akka.Remote`, `Akka.Serialization.Hyperion` and `Akka.TestKit` 1.4.25. |
| `PA2/bin/`, `PA2/obj/` | Committed Debug build output (net7.0). |

Messages:

| Union | Cases |
| --- | --- |
| `PeerMessages` | `Create(noOfBits, peerID, isFirst)`, `Join of bigint`, `FindSuccessor(startingPeerID, key, query, fingerIndex, hops)`, `FoundSuccessor`, `UpdateFingerTable(successor, fingerIndex)`, `FoundKey of hops`, `Maintain`, `SendMsgToPredecessor`, `PredecessorReceiveMsg`, `Alert`, `FingersToBeFixed`, `ReqCount` |
| `SimulatorMessages` | `SimulatorInit(nPeers, nRequests)`, `UpdateHops of hops`, `PrintHops` |

Key design points:

- **One `FindSuccessor` for three purposes.** The `query` field selects what the answering node sends back to `startingPeerID`: `1` = join (reply `FoundSuccessor`), `2` = finger fix (reply `UpdateFingerTable`), `3` = key lookup (reply `FoundKey hops`). When the key is not in the current node's range the request is forwarded to the closest preceding finger (`Seq.tryFindBack` over `fingersTable`) with `hops + 1`.
- **Stabilize / notify.** `Maintain` makes a peer ask its successor (`SendMsgToPredecessor`) for the successor's predecessor; `PredecessorReceiveMsg` adopts that node as the new successor if it lies in between, then `Alert` tells the new successor to update its predecessor. This is Chord's `stabilize()` + `notify()` pair.
- **Fix fingers.** Each `FingersToBeFixed` tick advances a per-peer index `i` and looks up `(peerID + 2^(i-1)) mod 2^160` with `query = 2`, so finger `i` is refreshed roughly every `160 * 100 ms`.
- **Addressing.** Peers never hold `IActorRef`s to each other; every message goes through `system.ActorSelection("akka://system/user/<id>")`, which is why actor names are the decimal IDs.
- **Completion.** Each peer counts down `ReqCount`; when its own requests are done it sends `UpdateHops` to the `Simulator`, which prints the average once every peer has reported and flips a `stop` flag that the main thread is polling.

Join sequence for one new peer `p` (as the code performs it):

1. `Create(160, p, false)` initialises `p` with an empty 160-entry finger table and no successor.
2. `Join firstPeer` sends `FindSuccessor(p, p, query = 1, 0, 0)` to the first peer, which routes it around the ring.
3. The answering node replies `FoundSuccessor s`, and `p` records `s` as its successor.
4. From then on the 100 ms `Maintain` and `FingersToBeFixed` ticks pull `p` into the ring and fill its fingers.

## Running it

Requires the .NET 7 SDK (or a newer SDK with .NET 7 targeting). Not verified on this machine (`dotnet` is not installed here).

```sh
cd PA2
dotnet run -- <numNodes> <numRequests>

dotnet run -- 100 10
```

Expected output is a single line, for example `The average hop count for 100 peers and 10 requests is <x>`, after which the program exits.

## Limitations / Notes

- Known issue: the termination test in `FindSuccessor` mixes `&&` and `||` without parentheses, so the wrap-around clause applies to every node and most lookups resolve at hop 0 while a node with an unset successor forwards to itself. Wrapping the two cases in parentheses, `(peerID < successor && key > peerID && key <= successor) || (peerID >= successor && (key > peerID || key <= successor))`, restores the intended interval check; until then the printed average should be read as a timing/convergence signal rather than an `O(log N)` hop count.
- All lookups are fired immediately after the join messages, while `Maintain` and `FingersToBeFixed` are still converging the ring, so results vary from run to run.
- The main thread busy-waits (`while not stop do x <- 1`) to keep the process alive, which pins one core until the simulator prints.
- There is no key storage, node departure or failure handling; `Akka.Remote`, `Akka.Serialization.Hyperion` and `Akka.TestKit` are referenced but unused (all actors are local). The comment near the top says `2^60` keys; the code uses 160 bits.
- `bin/` and `obj/` were committed with the source; a `.gitignore` has been added so future builds do not add more.
