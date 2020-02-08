package org.dist.mysimplekafka

import org.scalatest.FunSuite

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, TestSocketServer}
import org.dist.util.Networks

class MyZookeeperClientTest extends ZookeeperTestHarness {

  test("should register broker") {

    val broker = Broker(1, "master", 1010)

    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
    zookeeperClient.registerBroker(broker)

    assert(zookeeperClient.getAllBrokerIds().size == 1)
  }

}
