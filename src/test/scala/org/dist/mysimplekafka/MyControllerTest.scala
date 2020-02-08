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

  //  test("controller should subscribe to topic creation") {
  //    val brokerOne = Broker(1, "broker_1", 1010)
  //    val brokerTwo = Broker(2, "broker_2", 1011)
  //
  //    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
  //    val myCreateTopicCommand = new MyCreateTopicCommand(zookeeperClient)
  //
  //    zookeeperClient.registerBroker(brokerOne)
  //    zookeeperClient.registerBroker(brokerTwo)
  //
  //    val myController1 = new MyController(zookeeperClient, brokerOne.id)
  //    myController1.startUp()
  //
  //    val myController2 = new MyController(zookeeperClient, brokerTwo.id)
  //    myController2.startUp()
  //
  //    myCreateTopicCommand.createTopic("someTopicName",2,1)
  //
  //    TestUtils.waitUntilTrue(() => {
  //      myController1.name == "leader" && myController2.name == ""
  //    }, "waiting for callback", 2000)
  //
  //    assert(myController1.name == "leader")
  //    assert(myController2.name == "")
  //  }

  test("should elect a leader for all partitions") {
    val brokerOne = Broker(1, "broker_1", 1010)
    val brokerTwo = Broker(2, "broker_2", 1011)

    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
    val myCreateTopicCommand = new MyCreateTopicCommand(zookeeperClient)

    zookeeperClient.registerBroker(brokerOne)
    zookeeperClient.registerBroker(brokerTwo)

    val myController1 = new MyController(zookeeperClient, brokerOne.id)
    myController1.startUp()

    val myController2 = new MyController(zookeeperClient, brokerTwo.id)
    myController2.startUp()

    myCreateTopicCommand.createTopic("someTopicName", 2, 1)

    Thread.sleep(5000)

    assert(zookeeperClient.getPartitionReplicaLeaderInfo("someTopicName").size == 2)
  }
}

