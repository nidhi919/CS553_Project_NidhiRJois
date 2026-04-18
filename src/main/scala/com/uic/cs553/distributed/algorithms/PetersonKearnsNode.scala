package com.uic.cs553.distributed.algorithms

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import scala.concurrent.duration._
import scala.collection.mutable

//importing the same from netgamesim
import com.uic.cs553.netgamesim.{DistributedMessage, InitMsg, MessageMsg}


// Peterson-Kearns Message Types


case class CheckpointMsg(seqNum: Int, nodeId: Int, depVector: Map[Int, Int])
case class AckCheckpoint(seqNum: Int, fromNodeId: Int)
case class RequestRollback(targetSeqNum: Int, failedNodeId: Int, reason: String)
case class RollbackTo(targetSeqNum: Int, initiatorId: Int)
case class RollbackComplete(seqNum: Int, nodeId: Int)
case class Heartbeat(nodeId: Int, seqNum: Int)


// Peterson-Kearns Rollback Recovery Node


class PetersonKearnsNode(
                          nodeId: Int,
                          context: ActorContext[DistributedMessage],
                          timers: TimerScheduler[DistributedMessage]
                        ) extends AbstractBehavior[DistributedMessage](context) {

  // Timer message strings (defined inside the class)
  private val TIMER_CHECKPOINT = "TIMER_CHECKPOINT"
  private val TIMER_HEARTBEAT = "TIMER_HEARTBEAT"
  private val TIMER_FAILURE_CHECK = "TIMER_FAILURE_CHECK"


  // State


  private var neighbors: Map[Int, ActorRef[DistributedMessage]] = Map.empty

  private case class StoredCheckpoint(seqNum: Int, state: Map[String, Any], timestamp: Long)
  private val checkpoints: mutable.Map[Int, StoredCheckpoint] = mutable.Map.empty
  private var currentSeqNum: Int = 0
  private var lastCheckpointSeqNum: Int = 0

  private val dependencyVector: mutable.Map[Int, Int] = mutable.Map.empty
  private var messagesProcessed: Long = 0

  private var isRollingBack: Boolean = false
  private var rollbackTarget: Option[Int] = None
  private val rollbackAcks: mutable.Set[Int] = mutable.Set.empty

  private val lastHeartbeat: mutable.Map[Int, Long] = mutable.Map.empty
  private val HEARTBEAT_INTERVAL_MS = 2000
  private val FAILURE_TIMEOUT_MS = 5000
  private val CHECKPOINT_INTERVAL_MS = 10000

  private var rollbacksPerformed: Int = 0


  // Initialization


  override def onMessage(msg: DistributedMessage): Behavior[DistributedMessage] = {
    msg match {
      case InitMsg(nbrs, _, _, _, _) =>
        neighbors = nbrs
        dependencyVector += (nodeId -> 0)
        neighbors.keys.foreach { nbr =>
          dependencyVector += (nbr -> 0)
          lastHeartbeat += (nbr -> System.currentTimeMillis())
        }

        context.log.info(s"PK Node $nodeId initialized with ${neighbors.size} neighbors")

        // Start timers - send MessageMsg to self
        timers.startTimerAtFixedRate(MessageMsg(nodeId, TIMER_CHECKPOINT, ""), CHECKPOINT_INTERVAL_MS.millis)
        timers.startTimerAtFixedRate(MessageMsg(nodeId, TIMER_HEARTBEAT, ""), HEARTBEAT_INTERVAL_MS.millis)
        timers.startTimerAtFixedRate(MessageMsg(nodeId, TIMER_FAILURE_CHECK, ""), FAILURE_TIMEOUT_MS.millis)

        takeCheckpoint()

        Behaviors.same

      case MessageMsg(senderId, msgType, payload) =>
        if (senderId == nodeId) {
          // Internal timer messages
          msgType match {
            case TIMER_CHECKPOINT =>
              takeCheckpoint()
            case TIMER_HEARTBEAT =>
              sendHeartbeat()
            case TIMER_FAILURE_CHECK =>
              checkForFailures()
            case _ =>
              handleMessage(senderId, msgType, payload)
          }
        } else {
          handleMessage(senderId, msgType, payload)
        }
        Behaviors.same

      case _ =>
        Behaviors.same
    }
  }


  // Message Handling


  private def handleMessage(senderId: Int, msgType: String, payload: Any): Unit = {
    msgType match {
      case "PK_DATA" =>
        messagesProcessed += 1
        dependencyVector(senderId) = dependencyVector.getOrElse(senderId, 0) + 1
        if (messagesProcessed % 100 == 0) {
          context.log.info(s"Node $nodeId: Processed $messagesProcessed messages")
        }

      case "PK_CHECKPOINT" =>
        payload match {
          case CheckpointMsg(seqNum, fromNodeId, depVector) =>
            handleCheckpoint(seqNum, fromNodeId, depVector, senderId)
        }

      case "PK_ACK" =>
        payload match {
          case AckCheckpoint(seqNum, fromNodeId) =>
            handleAck(seqNum, fromNodeId)
        }

      case "PK_ROLLBACK_REQUEST" =>
        payload match {
          case RequestRollback(targetSeqNum, failedNodeId, reason) =>
            handleRollbackRequest(targetSeqNum, failedNodeId, reason, senderId)
        }

      case "PK_ROLLBACK" =>
        payload match {
          case RollbackTo(targetSeqNum, initiatorId) =>
            handleRollback(targetSeqNum, initiatorId, senderId)
        }

      case "PK_ROLLBACK_COMPLETE" =>
        payload match {
          case RollbackComplete(seqNum, fromNodeId) =>
            handleRollbackComplete(seqNum, fromNodeId)
        }

      case "PK_HEARTBEAT" =>
        payload match {
          case Heartbeat(fromNodeId, _) =>
            lastHeartbeat(fromNodeId) = System.currentTimeMillis()
        }

      case _ =>
    }
  }


  // Checkpointing


  private def takeCheckpoint(): Unit = {
    if (isRollingBack) return

    currentSeqNum += 1

    val state = Map(
      "messagesProcessed" -> messagesProcessed,
      "dependencyVector" -> dependencyVector.toMap,
      "currentSeqNum" -> currentSeqNum
    )

    val checkpoint = StoredCheckpoint(currentSeqNum, state, System.currentTimeMillis())
    checkpoints += (currentSeqNum -> checkpoint)
    lastCheckpointSeqNum = currentSeqNum

    context.log.info(s" Node $nodeId: Taking checkpoint #$currentSeqNum")
    broadcast("PK_CHECKPOINT", CheckpointMsg(currentSeqNum, nodeId, dependencyVector.toMap))
  }

  private def handleCheckpoint(seqNum: Int, fromNodeId: Int, depVector: Map[Int, Int], senderId: Int): Unit = {
    dependencyVector(fromNodeId) = depVector.getOrElse(nodeId, 0)
    context.log.debug(s"Node $nodeId: Received checkpoint #$seqNum from node $fromNodeId")
    sendToNeighbor(senderId, "PK_ACK", AckCheckpoint(seqNum, nodeId))
  }

  private def handleAck(seqNum: Int, fromNodeId: Int): Unit = {
    context.log.debug(s"Node $nodeId: Received ACK for checkpoint #$seqNum from $fromNodeId")
  }


  // Failure Detection


  private def sendHeartbeat(): Unit = {
    broadcast("PK_HEARTBEAT", Heartbeat(nodeId, currentSeqNum))
  }

  private def checkForFailures(): Unit = {
    val now = System.currentTimeMillis()
    val failedNodes = lastHeartbeat.filter { case (nid, lastTime) =>
      nid != nodeId && (now - lastTime) > FAILURE_TIMEOUT_MS
    }.keys.toList

    if (failedNodes.nonEmpty && !isRollingBack) {
      failedNodes.foreach { failedNode =>
        context.log.warn(s"Node $nodeId: Detected failure of node $failedNode")
        initiateRollback(failedNode)
      }
    }
  }


  // Rollback Recovery


  private def initiateRollback(failedNodeId: Int): Unit = {
    if (isRollingBack) return

    val targetSeqNum = lastCheckpointSeqNum

    context.log.warn(s"Node $nodeId: Initiating rollback to checkpoint #$targetSeqNum due to node $failedNodeId failure")

    isRollingBack = true
    rollbackTarget = Some(targetSeqNum)
    rollbackAcks.clear()
    rollbackAcks += nodeId

    broadcast("PK_ROLLBACK_REQUEST", RequestRollback(targetSeqNum, failedNodeId, s"Node $failedNodeId failed"))
    performRollback(targetSeqNum)
  }

  private def handleRollbackRequest(targetSeqNum: Int, failedNodeId: Int, reason: String, senderId: Int): Unit = {
    if (isRollingBack) return

    context.log.warn(s"Node $nodeId: Received rollback request to checkpoint #$targetSeqNum from $senderId")

    isRollingBack = true
    rollbackTarget = Some(targetSeqNum)
    rollbackAcks.clear()
    rollbackAcks += nodeId

    performRollback(targetSeqNum)
    sendToNeighbor(senderId, "PK_ROLLBACK_COMPLETE", RollbackComplete(targetSeqNum, nodeId))
  }

  private def handleRollback(targetSeqNum: Int, initiatorId: Int, senderId: Int): Unit = {
    if (!isRollingBack) {
      isRollingBack = true
      rollbackTarget = Some(targetSeqNum)
      performRollback(targetSeqNum)
    }
    sendToNeighbor(senderId, "PK_ROLLBACK_COMPLETE", RollbackComplete(targetSeqNum, nodeId))
  }

  private def handleRollbackComplete(seqNum: Int, fromNodeId: Int): Unit = {
    rollbackAcks += fromNodeId

    if (rollbackAcks.size >= neighbors.size + 1) {
      context.log.info(s"Node $nodeId: Rollback complete! All nodes recovered to checkpoint #$seqNum")
      isRollingBack = false
      rollbackTarget = None
      rollbacksPerformed += 1
    }
  }

  private def performRollback(targetSeqNum: Int): Unit = {
    checkpoints.get(targetSeqNum) match {
      case Some(checkpoint) =>
        messagesProcessed = checkpoint.state.getOrElse("messagesProcessed", 0L).asInstanceOf[Long]
        dependencyVector.clear()
        checkpoint.state.get("dependencyVector") match {
          case Some(dv: Map[Int, Int] @unchecked) =>
            dependencyVector ++= dv
          case _ =>
        }
        currentSeqNum = targetSeqNum
        lastCheckpointSeqNum = targetSeqNum
        context.log.info(s"Node $nodeId: Restored to checkpoint #$targetSeqNum")

      case None =>
        context.log.error(s"Node $nodeId: Checkpoint #$targetSeqNum not found!")
        messagesProcessed = 0
        currentSeqNum = 0
        lastCheckpointSeqNum = 0
    }
  }


  // Helper Methods


  private def sendToNeighbor(neighborId: Int, msgType: String, payload: Any): Unit = {
    neighbors.get(neighborId).foreach { ref =>
      ref ! MessageMsg(nodeId, msgType, payload)
    }
  }

  private def broadcast(msgType: String, payload: Any): Unit = {
    neighbors.values.foreach { ref =>
      ref ! MessageMsg(nodeId, msgType, payload)
    }
  }
}



object PetersonKearnsNode {
  def apply(nodeId: Int): Behavior[DistributedMessage] = {
    Behaviors.setup { ctx =>
      Behaviors.withTimers { timers =>
        new PetersonKearnsNode(nodeId, ctx, timers)
      }
    }
  }
}