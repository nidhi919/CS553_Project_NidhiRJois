package com.uic.cs553.netgamesim

object TestParser extends App {
  println("=" * 50)
  println("Testing DOT Parser")
  println("=" * 50)

  val graph = DotGraphParser.parseDotFile("graph.dot")

  println(s"\n Graph Statistics:")
  println(s"   Total nodes: ${graph.nodes.size}")
  println(s"   Total edges: ${graph.edges.size}")

  println(s"\n Timer nodes:")
  graph.nodes.values.filter(_.isTimer).foreach { node =>
    println(s"   Node ${node.id} (ticks every ${node.timerIntervalMs}ms)")
  }

  println(s"\n Test completed!")
}