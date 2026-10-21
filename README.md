# Chord Distributed Hash Table Simulator

[![CI](https://github.com/rohanreddy79/Chord-distributed-hash-table-/actions/workflows/ci.yml/badge.svg)](https://github.com/rohanreddy79/Chord-distributed-hash-table-/actions/workflows/ci.yml)

**An F# / Akka.NET simulation of the Chord peer-to-peer lookup protocol: peers with 160-bit SHA-1 identifiers join a ring, maintain successor pointers and finger tables, and the program reports the average number of hops a key lookup takes.**

## Overview

Chord (Stoica et al., 2001) places nodes on an identifier ring and routes a lookup for key `k` to the first node whose ID succeeds `k`, using a finger table so that each hop roughly halves the remaining distance. This project implements that protocol as Akka.NET actors in a single F# file: every peer is an actor named after its numeric ID, new peers join through the first peer, and two background loops drive Chord's `stabilize` and `fix_fingers` maintenance every 100 ms. After all peers have been told to join and the ring has had two full finger-refresh passes to settle, every peer issues `numRequests` lookups for random keys; a `Simulator` actor sums the hop counts, prints the average and the program exits. It targets .NET 8 (retargeted from .NET 7 in 2026-10) with Akka 1.4.25 and was written as a team programming assignment ("PA2") for COP 5615, Distributed Operating System Principles, at the University of Florida in fall 2023 — the same course and team as the companion `concurrent-client-server` and `distributed-computing-model` repositories.

## What it does

- Takes two command-line arguments, `<numNodes> <numRequests>`, and spawns one `peer` actor per node plus one `Simulator` actor.
- Generates 160-bit identifiers (`numberGenerator.nRandomForId`): SHA-1 of 20 random bytes, converted to a `bigint` by `getDecimalValues`. The bit length is computed as `(2^4) * 10 = 160`, so the ring has `2^160` positions.
- Joins peers one at a time: the first peer is its own successor and predecessor; each later peer sends `Join firstPeer`, which resolves its successor through `FindSuccessor`.
- Runs Chord maintenance continuously from two `async` loops: `Maintain` (stabilize — ask your successor for its predecessor and adopt it if it lies between you) and `FingersToBeFixed` (fix one finger entry per tick, cycling through all 160).
- Waits 32 s (two complete `fix_fingers` passes of 160 fingers × 100 ms) for the ring to stabilise, then resolves `numRequests` random keys from every peer, counting hops per lookup, prints `The average hop count for <N> peers and <R> requests is <x>` and exits.

## How it works

| File | Role |
| --- | --- |
| `PA2/Program.fs` | Everything: message types, `numberGenerator`, the `simulator` and `peer` actors, and the top-level script that spawns peers, starts the maintenance loops, waits for stabilisation and fires the lookups. |
| `PA2/PA2.fsproj` | .NET 8 console project referencing `Akka` and `Akka.FSharp` 1.4.25. |
| `.github/workflows/ci.yml` | GitHub Actions: `dotnet build` and a `dotnet run -- 20 5` smoke run on `ubuntu-latest`. |

Messages:

| Union | Cases |
| --- | --- |
| `PeerMessages` | `Create(noOfBits, peerID, isFirst)`, `Join of bigint`, `FindSuccessor(startingPeerID, key, query, fingerIndex, hops)`, `FoundSuccessor`, `UpdateFingerTable(successor, fingerIndex)`, `FoundKey of hops`, `Maintain`, `SendMsgToPredecessor`, `PredecessorReceiveMsg`, `Alert`, `FingersToBeFixed`, `ReqCount` |
| `SimulatorMessages` | `SimulatorInit(nPeers, nRequests)`, `UpdateHops of hops`, `PrintHops` |

Key design points:

- **One `FindSuccessor` for three purposes.** The `query` field selects what the answering node sends back to `startingPeerID`: `1` = join (reply `FoundSuccessor`), `2` = finger fix (reply `UpdateFingerTable`), `3` = key lookup (reply `FoundKey hops`). A node answers when the key lies in `(peerID, successor]` (with wrap-around); otherwise the request is forwarded with `hops + 1` to the closest preceding finger (`Seq.tryFindBack` over `fingersTable`), or to the successor when no finger applies yet. Only a peer that has not joined the ring retries at itself.
- **Stabilize / notify.** `Maintain` makes a peer ask its successor (`SendMsgToPredecessor`) for the successor's predecessor; `PredecessorReceiveMsg` adopts that node as the new successor if it lies in between, then `Alert` tells the new successor to update its predecessor. This is Chord's `stabilize()` + `notify()` pair.
- **Fix fingers.** Each `FingersToBeFixed` tick advances a per-peer index `i` and looks up `(peerID + 2^(i-1)) mod 2^160` with `query = 2`, so finger `i` is refreshed roughly every `160 * 100 ms`.
- **Addressing.** Peers never hold `IActorRef`s to each other; every message goes through `system.ActorSelection("akka://system/user/<id>")`, which is why actor names are the decimal IDs.
- **Completion.** Each peer counts down `ReqCount`; when its own requests are done it sends `UpdateHops` to the `Simulator`, which prints the average once every peer has reported and flips a `stop` flag that the main thread polls every 50 ms. The main thread then cancels the maintenance loops and terminates the actor system, so the process exits.

Join sequence for one new peer `p` (as the code performs it):

1. `Create(160, p, false)` initialises `p` with an empty 160-entry finger table and no successor.
2. `Join firstPeer` sends `FindSuccessor(p, p, query = 1, 0, 0)` to the first peer, which routes it around the ring.
3. The answering node replies `FoundSuccessor s`, and `p` records `s` as its successor.
4. From then on the 100 ms `Maintain` and `FingersToBeFixed` ticks pull `p` into the ring and fill its fingers.
5. After all joins have been issued the main thread sleeps `2 * 160 * 100` ms before firing the lookups: fingers fixed during the first pass, while the ring is still forming, are refreshed by the second.

## Running it

Requires the .NET 8 SDK. Verified on macOS arm64 with .NET SDK 8.0.426:

```sh
cd PA2
dotnet run -- <numNodes> <numRequests>
```

```
$ dotnet run -- 20 5
The average hop count for 20 peers and 5 requests is 1.950000

$ dotnet run -- 100 10
The average hop count for 100 peers and 10 requests is 3.308000
```

Each run takes about 35 s (the 32 s stabilisation wait plus build) and exits by itself. IDs and keys are random, so the average varies a little between runs: over six runs it ranged 1.95–2.55 for 20 peers and 2.98–3.31 for 100 peers, close to the `½·log₂N` Chord predicts (2.2 and 3.3). `dotnet build` prints NuGet warning NU1902 for the pinned Akka 1.4.25 (see Limitations); it is a warning only.

CI (`.github/workflows/ci.yml`) runs `dotnet build` and `dotnet run -- 20 5` with `actions/setup-dotnet@v4` on `ubuntu-latest`.

## Limitations / Notes

- The stabilisation wait is a fixed 32 s: the code has no "ring is stable" signal. Stabilisation needs roughly `N` 100 ms ticks, so with many hundreds of peers the lookups still start on a partially converged ring and the average creeps above `log N`.
- There is no key storage, node departure or failure handling; all actors are local.
- `Akka` and `Akka.FSharp` are kept at the author's pinned 1.4.25; NuGet flags advisory GHSA-gpv5-rp6w-58r8 (moderate) against it.
- `Environment.GetCommandLineArgs()` is indexed directly, so running without the two arguments throws an `IndexOutOfRangeException` rather than printing usage.

## Changelog

- 2026-10: fixed the `FindSuccessor` interval test (an unparenthesised `&&`/`||` made every node claim every key); lookups with no applicable finger are forwarded to the successor instead of back to the same peer, and a 32 s stabilisation wait precedes the lookups — the printed average went from 300–800 hops to ≈2 for 20 peers; replaced the busy-wait with a 50 ms poll and made the process exit after printing; retargeted `net7.0` → `net8.0` and dropped the unused `Akka.Remote`, `Akka.Serialization.Hyperion` and `Akka.TestKit` references; untracked `bin/` and `obj/`; added MIT license and CI.
