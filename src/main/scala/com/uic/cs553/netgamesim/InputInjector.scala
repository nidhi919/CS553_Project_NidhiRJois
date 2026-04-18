package com.uic.cs553.netgamesim

import akka.actor.typed.ActorRef
import scala.io.Source
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class InputInjector(nodeActors: Map[Int, ActorRef[DistributedMessage]]) {

  private var scheduledTasks: List[Future[Unit]] = List.empty

  def injectNow(nodeId: Int, msgType: String, payload: String): Unit = {
    if (nodeActors.contains(nodeId)) {
      println(s" Injecting message to node $nodeId: $msgType -> $payload")
      nodeActors(nodeId) ! ExternalInputMsg(nodeId, msgType, payload, 0)
    } else {
      println(s" Error: Node $nodeId does not exist!")
    }
  }

  def injectAfter(nodeId: Int, msgType: String, payload: String, delayMs: Long): Unit = {
    val task = Future {
      Thread.sleep(delayMs)
      injectNow(nodeId, msgType, payload)
    }
    scheduledTasks = task :: scheduledTasks
  }

  def loadFromFile(filePath: String): Unit = {
    println(s"\n Loading external messages from: $filePath")
    val source = Source.fromFile(filePath)
    val lines = source.getLines().toList
    source.close()

    var lineNum = 0
    lines.foreach { line =>
      lineNum += 1
      val trimmed = line.trim
      if (trimmed.nonEmpty && !trimmed.startsWith("#")) {
        val parts = trimmed.split(",").map(_.trim)
        if (parts.length >= 4) {
          try {
            val nodeId = parts(0).toInt
            val msgType = parts(1)
            val payload = parts(2)
            val delayMs = parts(3).toLong

            injectAfter(nodeId, msgType, payload, delayMs)
            println(s"   Scheduled: node $nodeId, $msgType, delay=${delayMs}ms")
          } catch {
            case e: NumberFormatException =>
              println(s"    Line $lineNum: Invalid number format - $line")
          }
        } else {
          println(s"   Line $lineNum: Expected 4 columns, got ${parts.length}")
        }
      }
    }
    println(s"Loaded messages from file\n")
  }

  def startInteractiveMode(): Unit = {
    println("\n" + "=" * 60)
    println("INTERACTIVE INPUT MODE")
    println("=" * 60)
    println("Commands:")
    println("  inject <nodeId> <msgType> <payload>")
    println("  schedule <nodeId> <msgType> <payload> <delayMs>")
    println("  list")
    println("  quit")
    println("=" * 60)

    var running = true
    while (running) {
      print("\n> ")
      val input = scala.io.StdIn.readLine()
      if (input == null) running = false
      else {
        val parts = input.trim.split("\\s+")
        parts.headOption match {
          case Some("inject") if parts.length >= 4 =>
            try {
              val nodeId = parts(1).toInt
              val msgType = parts(2)
              val payload = parts.drop(3).mkString(" ")
              injectNow(nodeId, msgType, payload)
            } catch {
              case e: NumberFormatException => println("Invalid node ID")
            }

          case Some("schedule") if parts.length >= 5 =>
            try {
              val nodeId = parts(1).toInt
              val msgType = parts(2)
              val payload = parts(3)
              val delayMs = parts(4).toLong
              injectAfter(nodeId, msgType, payload, delayMs)
              println(s"Scheduled: node $nodeId, $msgType in ${delayMs}ms")
            } catch {
              case e: NumberFormatException => println("Invalid number format")
            }

          case Some("list") =>
            println(s"\nAvailable nodes (${nodeActors.size} total):")
            nodeActors.keys.toList.sorted.take(20).foreach { id =>
              println(s"  Node $id")
            }
            if (nodeActors.size > 20) println(s"  ... and ${nodeActors.size - 20} more")

          case Some("quit") =>
            println("Exiting interactive mode")
            running = false

          case Some(cmd) =>
            println(s"Unknown command: $cmd")
            println("Available: inject, schedule, list, quit")

          case None =>
        }
      }
    }
  }
}