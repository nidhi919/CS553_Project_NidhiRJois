package com.uic.cs553.distributed.algorithms

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.uic.cs553.netgamesim.DistributedMessage

class AlgorithmStructureSpec extends AnyFlatSpec with Matchers {

  "PaxosNode" should "have a factory method" in {
    val behavior = PaxosNode(0)
    behavior should not be (null)
  }

  "PetersonKearnsNode" should "have a factory method" in {
    val behavior = PetersonKearnsNode(0)
    behavior should not be (null)
  }

  "Paxos message types" should "be defined correctly" in {
    val prepare = PaxosPrepare(100, 1)
    prepare.proposalId should be (100)
    prepare.proposerId should be (1)

    val promise = PaxosPromise(100, 50, Some("value"), 2)
    promise.proposalId should be (100)
    promise.acceptedProposalId should be (50)

    val accept = PaxosAccept(100, "value", 1)
    accept.proposalId should be (100)
    accept.value should be ("value")

    val decided = PaxosDecided(100, "value", 1)
    decided.proposalId should be (100)
    decided.value should be ("value")
  }

  "Peterson-Kearns message types" should "be defined correctly" in {
    val checkpoint = CheckpointMsg(1, 0, Map(0 -> 1))
    checkpoint.seqNum should be (1)
    checkpoint.nodeId should be (0)

    val heartbeat = Heartbeat(0, 5)
    heartbeat.nodeId should be (0)
    heartbeat.seqNum should be (5)

    val rollbackRequest = RequestRollback(1, 5, "failure")
    rollbackRequest.targetSeqNum should be (1)
    rollbackRequest.failedNodeId should be (5)
  }
}