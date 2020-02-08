package org.dist.mysimplekafka

import org.I0Itec.zkclient.ZkClient
import org.dist.queue.ZookeeperTestHarness
import org.dist.queue.utils.ZkUtils.Broker

class MyCreateTopicCommandTest extends ZookeeperTestHarness {

  test("should create a topic") {
    val brokerOne = Broker(1, "broker_1", 1010)
    val brokerTwo = Broker(2, "broker_2", 1011)

    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
    val myCreateTopicCommand = new MyCreateTopicCommand(zookeeperClient)

    zookeeperClient.registerBroker(brokerOne)
    zookeeperClient.registerBroker(brokerTwo)

    myCreateTopicCommand.createTopic("someTopicName",2,1)
    assert(zookeeperClient.getPartitionAssignmentsFor("someTopicName").size == 2)
  }

}
