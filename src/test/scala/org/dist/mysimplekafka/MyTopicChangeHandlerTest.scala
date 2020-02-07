package org.dist.mysimplekafka

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}

class MyTopicChangeHandlerTest extends ZookeeperTestHarness {

  test("should receive a callback for topic creation") {
    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)

    val brokerOne = Broker(1, "broker1", 1010)
    val brokerTwo = Broker(2, "broker2", 1012)
    zookeeperClient.registerBroker(brokerOne)
    zookeeperClient.registerBroker(brokerTwo)

    val myController = new MyController(zookeeperClient, brokerOne.id)
    val myTopicChangeHandler = new MyTopicChangeHandler(zookeeperClient, myController.onTopicChange)
    zookeeperClient.subscribeTopicChangeListener(myTopicChangeHandler)

    val topicName = "someTopicName"
    val partitionCount = 2

    val myCreateTopicCommand = new MyCreateTopicCommand(zookeeperClient)
    myCreateTopicCommand.createTopic(topicName, partitionCount, 1)

    TestUtils.waitUntilTrue(() => {
      myTopicChangeHandler.partitionCount == partitionCount
    }, "waiting for callback", 2000)

    assert(myTopicChangeHandler.partitionCount == 2)
  }


}
