package org.dist.consensus.zab

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.TestUtils
import org.dist.util.Networks
import org.scalatest.FunSuite

class ZabTest extends FunSuite {
  test("leader should set new epoc") {
    val address = new Networks().ipv4Address
    val peerAddr1 = InetAddressAndPort(address, 8080)
    val peerAddr2 = InetAddressAndPort(address, 8081)
    val peerAddr3 = InetAddressAndPort(address, 8082)

    val serverAddr1 = InetAddressAndPort(address, 9080)
    val serverAddr2 = InetAddressAndPort(address, 9081)
    val serverAddr3 = InetAddressAndPort(address, 9082)

    val serverList = List(QuorumServer(1, peerAddr1, serverAddr1), QuorumServer(2, peerAddr2, serverAddr2), QuorumServer(3, peerAddr3, serverAddr3))

    val config1 = QuorumPeerConfig(1, peerAddr1, serverAddr1, serverList, TestUtils.tempDir().getAbsolutePath)
    val peer1 = new QuorumPeer(config1, new QuorumConnectionManager(config1))

    val config2 = QuorumPeerConfig(2, peerAddr2, serverAddr2, serverList, TestUtils.tempDir().getAbsolutePath)
    val peer2 = new QuorumPeer(config2, new QuorumConnectionManager(config2))

    val leader = new Leader(peer2)
    val epoch1 = leader.newEpoch(0)

    assert(epoch1 == 1)
    val zxid1 = leader.newZxid(epoch1)

    val epoch2 = leader.newEpoch(zxid1)
    assert(epoch2 == 2)

    val zxid2 = leader.newZxid(epoch2)

    val epoch3 = leader.newEpoch(zxid2)
    assert(epoch3 == 3)

    val zxid3 = leader.newZxid(epoch3)

    println(zxid1)
    println(zxid2)
    println(zxid3)

  }

}