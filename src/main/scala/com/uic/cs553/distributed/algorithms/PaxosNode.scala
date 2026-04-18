package com.uic.cs553.distributed.algorithms

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import scala.concurrent.duration._
import scala.collection.mutable

// Importing the same DistributedMessage from NetGameSimRunner
import com.uic.cs553.netgamesim.{DistributedMessage, InitMsg, MessageMsg}


// Paxos Message Types


case class PaxosPrepare(proposalId: Int, proposerId: Int)
case class PaxosPromise(proposalId: Int, acceptedProposalId: Int, acceptedValue: Option[String], fromNodeId: Int)
case class PaxosAccept(proposalId: Int, value: String, proposerId: Int)
case class PaxosAccepted(proposalId: Int, value: String, fromNodeId: Int)
case class PaxosDecided(proposalId: Int, value: String, fromNodeId: Int)
case class PaxosPropose(value: String)


// Paxos Node Actor


class PaxosNode(
                 nodeId: Int,
                 context: ActorContext[DistributedMessage],
                 timers: TimerScheduler[DistributedMessage]
               ) extends AbstractBehavior[DistributedMessage](context) {


  // State


  private var neighbors: Map[Int, ActorRef[DistributedMessage]] = Map.empty
  private var allowedTypes: Map[Int, Set[String]] = Map.empty

  // Paxos state
  private var highestPromisedId: Int = 0
  private var highestAcceptedId: Int = -1
  private var highestAcceptedValue: Option[String] = None
  private var activeProposalId: Option[Int] = None
  private var activeProposalValue: Option[String] = None
  private val promisesReceived: mutable.Set[Int] = mutable.Set.empty
  private var highestSeenValue: Option[(Int, String)] = None
  private val acceptsReceived: mutable.Set[Int] = mutable.Set.empty
  private var decidedValue: Option[String] = None

  // Statistics
  private var messagesReceived: Long = 0
  private var messagesSent: Long = 0

  // Configuration
  private val PROPOSAL_TIMEOUT_MS = 5000

  private def quorumSize: Int = {
    val totalNodes = neighbors.size + 1
    (totalNodes / 2) + 1
  }


  // Message Handler


  override def onMessage(msg: DistributedMessage): Behavior[DistributedMessage] = {
    msg match {
      case InitMsg(nbrs, allowed, _, _, _) =>
        neighbors = nbrs
        allowedTypes = allowed
        context.log.info(s"Paxos Node $nodeId initialized with ${neighbors.size} neighbors")
        Behaviors.same

      case MessageMsg(senderId, msgType, payload) =>
        messagesReceived += 1
        handleMessage(senderId, msgType, payload)
        Behaviors.same

      case _ =>
        Behaviors.same
    }
  }


  // Message Routing


  private def handleMessage(senderId: Int, msgType: String, payload: Any): Unit = {
    msgType match {
      case "PAXOS_PROPOSE" =>
        payload match {
          case PaxosPropose(value) =>
            startProposal(value)
        }

      case "PAXOS_PREPARE" =>
        payload match {
          case PaxosPrepare(proposalId, proposerId) =>
            handlePrepare(proposalId, proposerId, senderId)
        }

      case "PAXOS_PROMISE" =>
        payload match {
          case PaxosPromise(proposalId, acceptedId, acceptedVal, _) =>
            handlePromise(proposalId, acceptedId, acceptedVal, senderId)
        }

      case "PAXOS_ACCEPT" =>
        payload match {
          case PaxosAccept(proposalId, value, _) =>
            handleAccept(proposalId, value, senderId)
        }

      case "PAXOS_ACCEPTED" =>
        payload match {
          case PaxosAccepted(proposalId, value, _) =>
            handleAccepted(proposalId, value, senderId)
        }

      case "PAXOS_DECIDED" =>
        payload match {
          case PaxosDecided(proposalId, value, _) =>
            handleDecided(proposalId, value, senderId)
        }

      case _ =>
      // Ignore other message types
    }
  }


  // Send Helpers


  private def sendToNeighbor(neighborId: Int, msgType: String, payload: Any): Unit = {
    neighbors.get(neighborId).foreach { ref =>
      messagesSent += 1
      ref ! MessageMsg(nodeId, msgType, payload)
    }
  }

  private def broadcast(msgType: String, payload: Any): Unit = {
    neighbors.values.foreach { ref =>
      messagesSent += 1
      ref ! MessageMsg(nodeId, msgType, payload)
    }
  }

  // Phase 1a: Proposer sends Prepare


  private def startProposal(value: String): Unit = {
    if (decidedValue.isDefined) {
      context.log.info(s"Node $nodeId: Already decided on '${decidedValue.get}'")
      return
    }

    // Generate unique proposal ID using timestamp + nodeId
    val newProposalId = ((System.currentTimeMillis() / 1000).toInt * 1000) + nodeId

    activeProposalId = Some(newProposalId)
    activeProposalValue = Some(value)
    promisesReceived.clear()
    acceptsReceived.clear()
    highestSeenValue = None

    context.log.info(s"Node $nodeId: Sending Prepare with proposal $newProposalId for value '$value'")

    broadcast("PAXOS_PREPARE", PaxosPrepare(newProposalId, nodeId))

    // Set timeout using a custom message type
    timers.startSingleTimer(s"paxos-timeout-$newProposalId",
      MessageMsg(nodeId, "PAXOS_TIMEOUT", newProposalId), PROPOSAL_TIMEOUT_MS.millis)
  }


  // Phase 1b: Acceptor handles Prepare


  private def handlePrepare(proposalId: Int, proposerId: Int, senderId: Int): Unit = {
    if (decidedValue.isDefined) return

    if (proposalId > highestPromisedId) {
      highestPromisedId = proposalId
      context.log.debug(s"Node $nodeId: Promising for proposal $proposalId from node $proposerId")

      sendToNeighbor(senderId, "PAXOS_PROMISE",
        PaxosPromise(proposalId, highestAcceptedId, highestAcceptedValue, nodeId))
    } else {
      context.log.debug(s"Node $nodeId: Ignoring Prepare $proposalId (have $highestPromisedId)")
    }
  }


  // Phase 2a: Proposer handles Promises


  private def handlePromise(proposalId: Int, acceptedId: Int, acceptedVal: Option[String], fromNodeId: Int): Unit = {
    if (!activeProposalId.contains(proposalId)) return

    promisesReceived += fromNodeId

    // Track the highest accepted value from promises
    if (acceptedId > 0) {
      highestSeenValue = highestSeenValue match {
        case Some((existingId, _)) if acceptedId > existingId => Some((acceptedId, acceptedVal.getOrElse("")))
        case None => Some((acceptedId, acceptedVal.getOrElse("")))
        case _ => highestSeenValue
      }
    }

    context.log.debug(s"Node $nodeId: Promise from $fromNodeId (${promisesReceived.size}/$quorumSize)")

    if (promisesReceived.size >= quorumSize) {
      // Determine the value to propose
      val valueToPropose = highestSeenValue match {
        case Some((_, value)) => value
        case None => activeProposalValue.getOrElse("consensus_value")
      }

      context.log.info(s"Node $nodeId: Quorum reached! Sending Accept with value '$valueToPropose'")
      broadcast("PAXOS_ACCEPT", PaxosAccept(proposalId, valueToPropose, nodeId))
    }
  }


  // Phase 2b: Acceptor handles Accept


  private def handleAccept(proposalId: Int, value: String, senderId: Int): Unit = {
    if (decidedValue.isDefined) return

    if (proposalId >= highestPromisedId) {
      highestPromisedId = proposalId
      highestAcceptedId = proposalId
      highestAcceptedValue = Some(value)

      context.log.info(s"Node $nodeId: Accepting proposal $proposalId with value '$value'")
      sendToNeighbor(senderId, "PAXOS_ACCEPTED", PaxosAccepted(proposalId, value, nodeId))
    } else {
      context.log.debug(s"Node $nodeId: Rejecting Accept $proposalId (need >= $highestPromisedId)")
    }
  }


  // Phase 3: Proposer handles Accepteds


  private def handleAccepted(proposalId: Int, value: String, fromNodeId: Int): Unit = {
    if (!activeProposalId.contains(proposalId)) return

    acceptsReceived += fromNodeId

    context.log.debug(s"Node $nodeId: Accepted from $fromNodeId (${acceptsReceived.size}/$quorumSize)")

    if (acceptsReceived.size >= quorumSize) {
      context.log.info(s"Node $nodeId: CONSENSUS REACHED! Value = '$value'")
      decidedValue = Some(value)

      // Broadcast decision to all
      broadcast("PAXOS_DECIDED", PaxosDecided(proposalId, value, nodeId))

      // Cancel timeout
      timers.cancel(s"paxos-timeout-$proposalId")
      activeProposalId = None
    }
  }


  // Learn Phase: Handle Decided


  private def handleDecided(proposalId: Int, value: String, fromNodeId: Int): Unit = {
    if (decidedValue.isEmpty) {
      context.log.info(s"Node $nodeId: Learning decided value '$value' from node $fromNodeId")
      decidedValue = Some(value)
      activeProposalId = None
    }
  }


  // Public API


  def propose(value: String): Unit = {
    context.self ! MessageMsg(nodeId, "PAXOS_PROPOSE", PaxosPropose(value))
  }
}





object PaxosNode {
  def apply(nodeId: Int): Behavior[DistributedMessage] = {
    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        new PaxosNode(nodeId, ctx, timers)
      }
    }
  }
}