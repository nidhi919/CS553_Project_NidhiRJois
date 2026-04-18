package com.uic.cs553.netgamesim

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, PrintWriter}

class DotParserSpec extends AnyFlatSpec with Matchers {

  "DotGraphParser" should "parse a valid DOT file correctly" in {
    val testDotContent =
      """digraph {
        |  "0" -> "1";
        |  "1" -> "2";
        |  "2" -> "0";
        |}
      """.stripMargin

    val testFile = new File("test.dot")
    val writer = new PrintWriter(testFile)
    writer.write(testDotContent)
    writer.close()

    try {
      val graph = DotGraphParser.parseDotFile("test.dot")
      graph.nodes.size should be (3)
      graph.edges.size should be (3)
      graph.nodes.contains(0) should be (true)
      graph.nodes.contains(1) should be (true)
      graph.nodes.contains(2) should be (true)
    } finally {
      testFile.delete()
    }
  }

  it should "extract node IDs correctly" in {
    val testDotContent =
      """digraph {
        |  "10" -> "20";
        |  "20" -> "30";
        |  "30" -> "10";
        |}
      """.stripMargin

    val testFile = new File("nodes.dot")
    val writer = new PrintWriter(testFile)
    writer.write(testDotContent)
    writer.close()

    try {
      val graph = DotGraphParser.parseDotFile("nodes.dot")
      graph.nodes.keys should contain allOf (10, 20, 30)
    } finally {
      testFile.delete()
    }
  }
}