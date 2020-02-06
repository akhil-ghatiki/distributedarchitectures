package org.dist.mysimplekafka

import org.dist.queue.ZookeeperTestHarness
import org.dist.queue.utils.ZkUtils.Broker

class MyControllerTest extends ZookeeperTestHarness {

  test("should elect one broker as controller") {
    val brokerOne = Broker(1, "broker_1", 1010)
    val brokerTwo = Broker(2, "broker_2", 1011)

    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
    zookeeperClient.registerBroker(brokerOne)
    zookeeperClient.registerBroker(brokerTwo)

    val myController1 = new MyController(zookeeperClient, brokerOne.id)
    myController1.startUp()

    val myController2 = new MyController(zookeeperClient, brokerTwo.id)
    myController2.startUp()

    assert(myController1.currentLeader == brokerOne.id)
    assert(myController2.currentLeader == brokerOne.id)
  }
}

