package com.uic.cs553.netgamesim

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import scala.concurrent.duration._


// Message Types


sealed trait DistributedMessage
final case class InitMsg(neighbors: Map[Int, ActorRef[DistributedMessage]], allowedTypes: Map[Int, Set[String]], pdf: Map[String, Double], isTimer: Boolean, interval: FiniteDuration) extends DistributedMessage
final case class MessageMsg(senderId: Int, msgType: String, payload: Any) extends DistributedMessage
final case class ExternalInputMsg(targetNodeId: Int, msgType: String, payload: Any, delayMs: Long) extends DistributedMessage
private case object TickMsg extends DistributedMessage
private case object ShutdownMsg extends DistributedMessage

// Simple Node Actor


class SimpleNode(nodeId: Int, context: ActorContext[DistributedMessage], timers: TimerScheduler[DistributedMessage])
  extends AbstractBehavior[DistributedMessage](context) {

  private var neighbors: Map[Int, ActorRef[DistributedMessage]] = Map.empty
  private var allowedTypes: Map[Int, Set[String]] = Map.empty      // Edge labels
  private var pdf: Map[String, Double] = Map.empty
  private var isTimerNode: Boolean = false
  private val rng = new scala.util.Random()

  // Statistics with BLOCKED counter
  private var messagesReceived: Long = 0
  private var messagesSent: Long = 0
  private var messagesBlocked: Long = 0      // NEW: Count blocked messages

  override def onMessage(msg: DistributedMessage): Behavior[DistributedMessage] = {
    msg match {
      case InitMsg(nbrs, allowed, pdfConfig, timer, interval) =>
        neighbors = nbrs
        allowedTypes = allowed
        pdf = pdfConfig
        isTimerNode = timer

        context.log.info(s"Node $nodeId initialized with ${neighbors.size} neighbors")

        // Log edge labels for debugging (shows what messages are allowed on each edge)
        allowedTypes.foreach { case (nbrId, types) =>
          context.log.info(s"  Edge $nodeId → $nbrId: allowed messages = ${types.mkString(", ")}")
        }

        if (isTimerNode) {
          context.log.info(s"Node $nodeId starting timer (sends messages every ${interval.toMillis}ms)")
          timers.startTimerAtFixedRate(TickMsg, interval)
        }
        Behaviors.same


      // EDGE LABEL ENFORCEMENT - ON RECEIVE

      case MessageMsg(senderId, msgType, payload) =>
        // Check if this message type is allowed from this sender
        val isAllowed = allowedTypes.get(senderId) match {
          case Some(allowedSet) => allowedSet.contains(msgType)
          case None =>
            context.log.warn(s"Node $nodeId: No edge defined from $senderId! Blocking message.")
            false
        }

        if (isAllowed) {
          // Message is allowed - process it
          messagesReceived += 1
          onMessageReceived(msgType, payload, senderId)

          if (messagesReceived % 100 == 0) {
            context.log.info(s" Node $nodeId received $messagesReceived messages (last: $msgType from $senderId)")
          }
        } else {
          // Message is NOT allowed - BLOCK it
          messagesBlocked += 1
          context.log.warn(s" Node $nodeId: BLOCKED '$msgType' from node $senderId (not allowed on this edge)")

          // Show summary every 10 blocks
          if (messagesBlocked % 10 == 0) {
            context.log.warn(s"Node $nodeId: Total blocked so far: $messagesBlocked")
          }
        }
        Behaviors.same


      // TIMER NODE - Sends messages with edge label checking

      case TickMsg =>
        if (isTimerNode && neighbors.nonEmpty) {
          // Sample message type from PDF
          val msgType = sampleMessageType()

          // Send to random neighbor (will be checked by receiver)
          val randomId = neighbors.keys.toList(rng.nextInt(neighbors.size))
          sendToNeighbor(randomId, msgType, s"tick from $nodeId")
        }
        Behaviors.same

      case ExternalInputMsg(targetNodeId, msgType, payload, delayMs) =>
        if (targetNodeId == nodeId) {
          context.log.info(s" Node $nodeId received EXTERNAL input: $msgType -> $payload")
          // Process external message just like a normal message (senderId = -1 for external)
          onMessageReceived(msgType, payload, -1)
        }
        Behaviors.same

      case ShutdownMsg =>
        // Print final statistics
        context.log.info(s" Node $nodeId FINAL STATS: sent=$messagesSent, received=$messagesReceived, blocked=$messagesBlocked")
        Behaviors.stopped

      case _ =>
        Behaviors.same
    }
  }


  // Helper Methods with SIDE enforcement


  private def sendToNeighbor(neighborId: Int, messageType: String, payload: Any): Unit = {
    neighbors.get(neighborId) match {
      case Some(neighborRef) =>
        // Check if we're allowed to send this message type to this neighbor
        val isAllowed = allowedTypes.get(neighborId) match {
          case Some(allowedSet) => allowedSet.contains(messageType)
          case None => false
        }

        if (isAllowed) {
          messagesSent += 1
          neighborRef ! MessageMsg(nodeId, messageType, payload)
        } else {
          messagesBlocked += 1
          context.log.warn(s" Node $nodeId: Cannot send '$messageType' to $neighborId (not allowed on edge)")
        }
      case None =>
        context.log.warn(s"Node $nodeId: No neighbor with ID $neighborId")
    }
  }

  private def sampleMessageType(): String = {
    if (pdf.isEmpty) return "PING"
    val rand = rng.nextDouble()
    var cumulative = 0.0
    for ((msgType, prob) <- pdf) {
      cumulative += prob
      if (rand <= cumulative) return msgType
    }
    pdf.keys.head
  }

  private def onMessageReceived(messageType: String, payload: Any, senderId: Int): Unit = {
    // You can override this in algorithm-specific nodes
    // For now, just log external messages specially
    if (senderId == -1) {
      context.log.info(s" Node $nodeId processed EXTERNAL message: $messageType -> $payload")
    } else {
      context.log.debug(s"Node $nodeId processed: $messageType from $senderId")
    }
  }

}


// Main Runner


object NetGameSimRunner {

  def main(args: Array[String]): Unit = {
    // Parse flags first
    val interactive = args.contains("--interactive")

    // Filter out flags to get positional arguments
    val nonFlags = args.filterNot(_ == "--interactive")

    val dotFilePath = nonFlags.lift(0).getOrElse("graph.dot")
    val runDuration = nonFlags.lift(1).getOrElse("30s")
    val algorithmName = nonFlags.lift(2).getOrElse("simple")
    val inputFile = nonFlags.lift(3)  // Optional input file

    println("=" * 60)
    println("NetGameSim + Akka Distributed Algorithms")
    println("=" * 60)
    println(s"Graph file: $dotFilePath")
    println(s"Run duration: $runDuration")
    println(s"Algorithm: $algorithmName")
    if (inputFile.isDefined) println(s"Input file: ${inputFile.get}")
    if (interactive) println("Interactive mode: ON")
    println("=" * 60)

    val graph = DotGraphParser.parseDotFile(dotFilePath)

    val system = ActorSystem[DistributedMessage](
      Behaviors.setup { ctx =>

        //val algorithmName = args.lift(2).getOrElse("simple")

        val nodeActors = graph.nodes.map { case (id, nodeConfig) =>
          val behavior = algorithmName.toLowerCase match {
            case "paxos" =>
              com.uic.cs553.distributed.algorithms.PaxosNode(id)
            case "pk" | "peterson" | "rollback" =>
              com.uic.cs553.distributed.algorithms.PetersonKearnsNode(id)
            case _ =>
              SimpleNode.create(id)
          }
          id -> ctx.spawn(behavior, s"node-$id")
        }.toMap

        graph.nodes.foreach { case (id, nodeConfig) =>
          val neighbors = graph.edges.filter(_.from == id).map(e => e.to -> nodeActors(e.to)).toMap
          val allowed = graph.edges.filter(_.from == id).map(e => e.to -> e.allowedMessageTypes).toMap

          nodeActors(id) ! InitMsg(
            neighbors = neighbors,
            allowedTypes = allowed,
            pdf = nodeConfig.pdf,
            isTimer = nodeConfig.isTimer,
            interval = nodeConfig.timerIntervalMs.millis
          )
        }

        // Create InputInjector
        val injector = new InputInjector(nodeActors)

        // Load input file if provided
        inputFile.foreach { file =>
          injector.loadFromFile(file)
        }

        // Start interactive mode if requested (runs in separate thread)
        if (interactive) {
          new Thread(() => injector.startInteractiveMode()).start()
        }

        val duration = parseDuration(runDuration)
        ctx.scheduleOnce(duration, ctx.self, ShutdownMsg)

        Behaviors.receiveMessage {
          case ShutdownMsg =>
            println(s"\n" + "=" * 60)
            println("SHUTTING DOWN SIMULATION")
            println("=" * 60)
            println("Input injection is ACTIVE")
            println("- External messages can be injected via file or interactive mode")
            println("- Check logs above for ' EXTERNAL input' messages")
            println("=" * 60)
            Behaviors.stopped
          case _ =>
            Behaviors.same
        }
      },
      "NetGameSimSystem"
    )

    Thread.sleep(parseDuration(runDuration).toMillis + 2000)
    system.terminate()
    println("\nSimulation completed!")
  }

  private def parseDuration(durationStr: String): FiniteDuration = {
    val value = durationStr.takeWhile(_.isDigit).toInt
    durationStr.last match {
      case 's' => value.seconds
      case 'm' => value.minutes
      case _ => value.seconds
    }
  }

  object SimpleNode {
    def create(nodeId: Int): Behavior[DistributedMessage] = {
      Behaviors.setup { ctx =>
        Behaviors.withTimers { timers =>
          new SimpleNode(nodeId, ctx, timers)
        }
      }
    }
  }

}