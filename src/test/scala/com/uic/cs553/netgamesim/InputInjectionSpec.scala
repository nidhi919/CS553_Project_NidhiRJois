package com.uic.cs553.netgamesim

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, PrintWriter}

class InputInjectionSpec extends AnyFlatSpec with Matchers {

  "InputInjector" should "parse CSV file correctly" in {
    val csvContent =
      """# Format: nodeId,messageType,payload,delayMs
        |5,PING,test1,1000
        |10,WORK,test2,2000
        |20,GOSSIP,test3,3000
      """.stripMargin

    val testFile = new File("test_input.csv")
    val writer = new PrintWriter(testFile)
    writer.write(csvContent)
    writer.close()

    try {
      testFile.exists() should be (true)
      val source = scala.io.Source.fromFile(testFile)
      val lines = source.getLines().filter(_.trim.nonEmpty).toList
      source.close()

      // Filter out comment lines
      val dataLines = lines.filterNot(_.startsWith("#"))

      dataLines should have size 3
      dataLines(0) should include ("5,PING,test1,1000")
      dataLines(1) should include ("10,WORK,test2,2000")
      dataLines(2) should include ("20,GOSSIP,test3,3000")
    } finally {
      testFile.delete()
    }
  }

  it should "skip comment lines" in {
    val csvContent =
      """# This is a comment
        |# Another comment
        |5,PING,test,1000
      """.stripMargin

    val testFile = new File("test_comments.csv")
    val writer = new PrintWriter(testFile)
    writer.write(csvContent)
    writer.close()

    try {
      val source = scala.io.Source.fromFile(testFile)
      val lines = source.getLines().filter(_.trim.nonEmpty).toList
      source.close()

      val dataLines = lines.filterNot(_.startsWith("#"))
      dataLines should have size 1
      dataLines.head should include ("5,PING,test,1000")
    } finally {
      testFile.delete()
    }
  }
}