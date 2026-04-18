# CS553 Project Report

**Student:** Nidhi Ravishankar Jois

**Professor:** Mark Grechanik

**Date:** April 18, 2026

---


## Design Decisions

**1. Using Akka Typed Actors**
I chose Akka typed actors. The compiler catches wrong message types. Akka also has built-in timers which I used for the timer node.

**2. DOT file as bridge between NetGameSim and Akka**
NetGameSim exports graphs as .dot files. I wrote a parser to read these files and extract nodes and edges. This keeps the two projects separate but they work together.

**3. Edge label enforcement on both send and receive**
I check message types when sending AND when receiving. If a message type is not allowed on that edge, it gets blocked and logged.

**4. PDF sampling for traffic generation**
The timer node uses a probability distribution to pick what message to send. This creates random traffic. The probabilities sum to 1.0.

**5. Two ways to inject external messages**
- File mode: read CSV with nodeId, messageType, payload, delayMs
- Interactive mode: type commands while program runs

---

## Configuration Choices

Settings in `application.conf`:

```hocon
netgamesim {
  graph-file = "graph.dot"
  run-duration = "30s"
  algorithm = "simple"
  seed = 42
  
  traffic {
    tick-interval-ms = 1000
    default-pdf = [
      { message-type = "PING", probability = 0.4 }
      { message-type = "WORK", probability = 0.3 }
      { message-type = "GOSSIP", probability = 0.2 }
      { message-type = "ACK", probability = 0.1 }
    ]
  }
}

```

## Results
Graph generated using netgamesim:

501 nodes

972 edges

Tests: 15 tests passed
