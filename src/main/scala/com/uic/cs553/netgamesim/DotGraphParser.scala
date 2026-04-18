package com.uic.cs553.netgamesim

import scala.io.Source

case class EnrichedNode(
                         id: Int,
                         pdf: Map[String, Double],
                         isTimer: Boolean,
                         timerIntervalMs: Long,
                         messageTypes: Set[String]
                       )

case class EnrichedEdge(
                         from: Int,
                         to: Int,
                         allowedMessageTypes: Set[String]
                       )

case class EnrichedGraph(
                          nodes: Map[Int, EnrichedNode],
                          edges: List[EnrichedEdge]
                        )

object DotGraphParser {

  def parseDotFile(filePath: String): EnrichedGraph = {
    println(s"Parsing DOT file: $filePath")

    val source = Source.fromFile(filePath)
    val lines = source.getLines().toList
    source.close()

    val edgePattern = """\s*"?(\d+)"?\s*->\s*"?(\d+)"?\s*;?""".r

    val nodesSet = scala.collection.mutable.Set[Int]()
    val edgesList = scala.collection.mutable.ListBuffer[(Int, Int)]()

    lines.foreach { line =>
      edgePattern.findAllMatchIn(line).foreach { m =>
        val from = m.group(1).toInt
        val to = m.group(2).toInt
        edgesList += ((from, to))
        nodesSet.add(from)
        nodesSet.add(to)
      }
    }

    // Define message type categories for edge labeling
    val controlOnly = Set("CONTROL")
    val pingWorkOnly = Set("PING", "WORK", "ACK")
    val gossipOnly = Set("GOSSIP")
    val allMessages = Set("PING", "WORK", "CONTROL", "ACK", "GOSSIP")

    // Build nodes map correctly
    var nodesMap = Map.empty[Int, EnrichedNode]
    for (id <- nodesSet) {
      val pdf = if (id == 0) {
        Map("PING" -> 0.5, "CONTROL" -> 0.3, "WORK" -> 0.2)
      } else if (id < 100) {
        Map("WORK" -> 0.5, "PING" -> 0.3, "ACK" -> 0.2)
      } else {
        Map("GOSSIP" -> 0.6, "PING" -> 0.4)
      }

      nodesMap += (id -> EnrichedNode(
        id = id,
        pdf = pdf,
        isTimer = (id == 0),
        timerIntervalMs = 1000,
        messageTypes = allMessages
      ))
    }

    // Build edges list
    val enrichedEdges = edgesList.toList.map { case (from, to) =>
      val allowed = if (from == 0) {
        controlOnly
      } else if (from % 2 == 0) {
        pingWorkOnly
      } else {
        gossipOnly ++ Set("PING")
      }
      EnrichedEdge(from, to, allowed)
    }

    println(s" Parsed ${nodesMap.size} nodes and ${enrichedEdges.size} edges")
    println("\nSample Edge Labels (showing which messages are allowed):")
    enrichedEdges.take(10).foreach { e =>
      println(s"   $e.from → $e.to: ${e.allowedMessageTypes.mkString(", ")}")
    }
    println()

    EnrichedGraph(nodesMap, enrichedEdges)
  }
}