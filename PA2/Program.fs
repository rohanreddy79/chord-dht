// #time "on"
// #r "nuget: Akka.Remote, 1.4.25"
// #r "nuget: Akka, 1.4.25"
// #r "nuget: Akka.FSharp, 1.4.25"
// #r "nuget: Akka.Serialization.Hyperion, 1.4.25"
// #r "nuget: Akka.TestKit, 1.4.25"

open System
open System.Security.Cryptography
open Akka.Actor
open Akka.FSharp
open System.Threading

// Using 160 bit identifiers so we have 2^60 keys 
// and we randomly generate noOfPeers ids and make them nodes.
// and randomly generate noOfReqs ids and find them to calulcate hops

// let noOfPeers = int(fsi.CommandLineArgs.[1])
// let noOfReqs = int(fsi.CommandLineArgs.[2])
let args = Environment.GetCommandLineArgs()
let noOfPeers = int(args.[1])
let noOfReqs = int(args.[2])

let dummyValue = bigint(Int64.MinValue)

let mutable stop = false

type SimulatorMessages = 
    | SimulatorInit of nPeers : int * nRequests : int
    | UpdateHops of hops : int
    | PrintHops

type PeerMessages = 
    | Create of noOfBits : int * peerID: bigint * isFirst : bool
    | FindSuccessor of startingPeerID : bigint * key : bigint * query : int * fingerIndex : int * hops : int
    | FoundSuccessor of bigint
    | UpdateFingerTable of successor : bigint * fingerIndex : int
    | SendMsgToPredecessor // send message to calling node(previous predecesso) with new predecessor 
    | PredecessorReceiveMsg of bigint
    | Alert of bigint
    | Join of bigint
    | Maintain
    | FoundKey of hops : int
    | ReqCount of int
    | FingersToBeFixed

let system = System.create "system" <| Configuration.load()

let akkaRouting = "akka://system/user/"

module numberGenerator = 
    let getDecimalValues (arr: byte[]) : bigint =
        // printf "In get decimal values";
        let mutable multiplicator = 1I
        let mutable twoN = 2I
        let mutable result = 0I
        for i in arr.Length-1 .. -1 .. 0 do
            let mutable bytN = int(arr.[i])
            for _ in 1..8 do
                let bit = bigint(bytN &&& 1)
                result <- result + (bit * multiplicator)
                multiplicator <- multiplicator * twoN
                bytN <- bytN >>> 1
        result

    let nRandomForId (num : int) : bigint[] =
        Array.init<bigint> num (fun _ -> 
            let num: byte[] = Array.zeroCreate 20
            Random().NextBytes(num)
            let hashed = SHA1.Create().ComputeHash(num)
            getDecimalValues(hashed)
        )

    let thePowerOfTwo (n : int) : bigint =
        bigint(Math.Pow(2.0, n |> float))

module mainFunction = 
    let SimulatorRef = system.ActorSelection(akkaRouting + "Simulator");
    let simulator (mailbox: Actor<_>) =
        let mutable noOfHops = 0
        let mutable noOfPeers = 0
        let mutable noOfTotalRequests = 0
        let mutable noOfCompletedPeers = 0 // peers who received noOfRequests

        let rec loop() = actor{
            let! message = mailbox.Receive()
            match message with

            | SimulatorInit(nPeers,nRequests) ->
                noOfPeers <- nPeers
                noOfTotalRequests <- nRequests
            
            | UpdateHops(hops) ->
                noOfHops <- noOfHops + hops
                noOfCompletedPeers <- noOfCompletedPeers + 1
                if noOfCompletedPeers = noOfPeers then
                    SimulatorRef <! PrintHops
            
            | PrintHops ->
                let result = double(noOfHops) / double(noOfPeers * noOfTotalRequests)
                printfn "The average hop count for %d peers and %d requests is %f" noOfPeers noOfTotalRequests result
                stop <- true
            
            return! loop()
        }
        loop()
    
    let peer( mailbox: Actor<_>) = 
        let mutable predecessor = bigint(-1)
        let mutable successor = bigint(-1)
        let mutable i = 0
        let mutable fingersTable : bigint[] = Array.empty
        let mutable peerID = bigint(0)
        let mutable totalPossibleIDs = bigint(0)
        let mutable idLen = 0 // we will use 160 bits ids
        let mutable totalHops = 0
        let mutable totalRequests = 0

        let cancelToken = new CancellationTokenSource()
        let rec loop() = actor {
            let! message = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with

            | Create(noOfBits,id,isFirst) ->
                peerID <- id
                totalPossibleIDs <- numberGenerator.thePowerOfTwo noOfBits
                idLen <- noOfBits
                fingersTable <- [|for _ in 1 .. idLen -> dummyValue|]
                if isFirst then
                    successor <- id; predecessor <- id
                else
                    successor <- dummyValue; predecessor <- dummyValue
            
            | FindSuccessor(startingPeerID, key,query,fingerIndex,hops) ->
                let found = ((peerID < successor) && ((key > peerID) && (key <= successor)) || ((key > peerID) || (key <= successor)))
                if found then
                    let originPeerRef = system.ActorSelection(akkaRouting + string(startingPeerID))
                    if query = 1 then originPeerRef <! FoundSuccessor(successor)
                    elif query = 2 then originPeerRef <! UpdateFingerTable(successor,fingerIndex)
                    elif query = 3 then originPeerRef <! FoundKey(hops)
                else
                    let ans = 
                        fingersTable |> Seq.tryFindBack(fun (s: bigint) -> 
                            (s <> dummyValue && ((peerID >= key && (s > peerID || ((s < peerID) && (s < key)))) || ((s > peerID) && (s < key)))))
                    let nextSuccessor = (if ans.IsNone then peerID else ans.Value)
                    let successorPeer = system.ActorSelection(akkaRouting + string(nextSuccessor))
                    successorPeer <! FindSuccessor(startingPeerID,key,query,fingerIndex,hops+1)
            
            | FoundSuccessor(succ) ->
                successor <- succ
            
            | SendMsgToPredecessor ->
                // send your new predecessor id to your old predecessor.
                if predecessor <> dummyValue then
                    sender.Tell(PredecessorReceiveMsg(predecessor))
            
            // Received succ (new predecessor of your sucessor  ) so we need to update our successor
            | PredecessorReceiveMsg(newSuccessor) ->
                let changeInSuccessor = ((peerID>= successor && ((newSuccessor > peerID) || (newSuccessor < successor))) || ((newSuccessor > peerID) && (newSuccessor < successor)))
                if changeInSuccessor then
                    successor <- newSuccessor
                let succPeer = system.ActorSelection(akkaRouting + string(successor))
                // Send alert to your new successor that u made him your successor and update their predecessor
                succPeer <! Alert(peerID)
            
            | Alert(newPredecessorID) ->
                let changeInPredecessor = ((predecessor = peerID) || (predecessor = dummyValue) || (predecessor >= peerID && ((newPredecessorID > predecessor) || (newPredecessorID < peerID))) || ((newPredecessorID > predecessor) && (newPredecessorID < peerID)))
                if changeInPredecessor then predecessor <- newPredecessorID
            
            | Join(chordPeer) ->
                let chordPeer = system.ActorSelection(akkaRouting + string(chordPeer))
                chordPeer <! FindSuccessor(peerID, peerID, 1, 0, 0)
            
            | Maintain ->
                // Ask your successor for its predecessor
                if successor <> dummyValue then
                    let succ = system.ActorSelection(akkaRouting + string(successor))
                    succ <! SendMsgToPredecessor 
            | ReqCount(n) ->
                totalRequests <- n
            
            | FingersToBeFixed ->
                // printfn "Fixing Fingers for Node : %A" (peerID%(100000|> bigint))
                // while i < idLen-1 do
                if successor <> dummyValue then
                    i <- i + 1
                    if i > idLen then
                        i <- 1

                    // calculate 2^(i-1)
                    let mutable v = numberGenerator.thePowerOfTwo (i - 1)
                    // n + 2^(i-1) % m
                    v <- (peerID + v) % totalPossibleIDs

                    let path = akkaRouting + string(peerID)
                    let currPeer = system.ActorSelection(path)
                    currPeer <! FindSuccessor(peerID, v, 2, i, 0)
            
            | UpdateFingerTable(succ, fingerIndex) ->
                fingersTable.[fingerIndex - 1] <- succ

            | FoundKey(hops) ->
                totalHops <- totalHops + hops
                totalRequests <- totalRequests - 1
                if totalRequests = 0 then
                    SimulatorRef <! UpdateHops(totalHops)

            return! loop()
        }
        loop()


let SimulatorRef =  spawn system "Simulator" <| mainFunction.simulator 
SimulatorRef <! SimulatorInit(noOfPeers, noOfReqs)
let mutable peers = []
let cancel = new CancellationTokenSource()
let fingerCancel = new CancellationTokenSource()
let mutable firstChordPeer = dummyValue

let maintain = async{
    while true do
        for peer in peers do
            let peerActor = system.ActorSelection(akkaRouting + peer)
            peerActor <! Maintain
        do! Async.Sleep 100
    }

let updateFinger = async{
    while true do
        for peer in peers do
            let peerActorRef = system.ActorSelection(akkaRouting + peer)
            peerActorRef <! FingersToBeFixed
        do! Async.Sleep 100
}

let funcSync = 
    Async.Start(maintain, cancel.Token)
    Async.Start(updateFinger, fingerCancel.Token)



let mutable x = 1
let nextPeer = numberGenerator.nRandomForId noOfPeers

let peerActor = spawn system (string(nextPeer.[0])) <| mainFunction.peer
peers <- peers @ [string(nextPeer.[0])]
firstChordPeer <- nextPeer.[0]
peerActor <! Create((Math.Pow(2.0, 4.0)|>int) * 10, nextPeer.[0], true)
peerActor <! ReqCount(noOfReqs) 

for i in 1..nextPeer.Length - 1 do
    let peerActor = spawn system (string(nextPeer.[i])) <| mainFunction.peer
    peers <- peers @ [string(nextPeer.[i])]
    peerActor <! Create((Math.Pow(2.0, 4.0)|>int) * 10, nextPeer.[i], false)
    peerActor <! Join(firstChordPeer)
    peerActor <! ReqCount(noOfReqs)

// for i in 0..nextPeer.Length-1 do
//     let p = system.ActorSelection(akkaRouting + string(nextPeer.[i-1]));
//     p <! FingersToBeFixed


let requests = numberGenerator.nRandomForId noOfReqs

for i in 0..nextPeer.Length - 1 do
    let peerRef = system.ActorSelection(akkaRouting + string(nextPeer.[i]))
    for j in 0..requests.Length - 1 do
        peerRef <! FindSuccessor(nextPeer.[i], requests.[j], (Math.Pow(2.0, 1.0)|>int) + 1, 0, 0)   

// do not terminate the program till we find the ans
while not stop do
    x <- 1
