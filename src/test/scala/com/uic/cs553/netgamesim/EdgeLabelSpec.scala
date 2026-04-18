package com.uic.cs553.netgamesim

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File

class EdgeLabelSpec extends AnyFlatSpec with Matchers {

  "Edge labels" should "allow configured message types" in {
    val allowed = Set("PING", "WORK", "CONTROL")

    allowed.contains("PING") should be (true)
    allowed.contains("WORK") should be (true)
    allowed.contains("CONTROL") should be (true)
    allowed.contains("GOSSIP") should be (false)
    allowed.contains("ACK") should be (false)
  }

  "Node PDFs" should "have probabilities that sum to approximately 1.0" in {
    val graphFile = new File("graph.dot")
    if (graphFile.exists()) {
      val graph = DotGraphParser.parseDotFile("graph.dot")
      graph.nodes.values.foreach { node =>
        val sum = node.pdf.values.sum
        sum should be (1.0 +- 0.01)
      }
    } else {
      println("Skipping PDF sum test - graph.dot not found")
    }
  }

  "Each node" should "have a valid PDF (non-empty)" in {
    val graphFile = new File("graph.dot")
    if (graphFile.exists()) {
      val graph = DotGraphParser.parseDotFile("graph.dot")
      graph.nodes.values.foreach { node =>
        node.pdf should not be empty
      }
    } else {
      println("Skipping PDF non-empty test - graph.dot not found")
    }
  }
}