package org.dist.mysimplekafka

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}


class MyBrokerChangeListenerTest extends ZookeeperTestHarness {

  test("should get a callback on broker registration and update the brokers size") {
    val broker = Broker(1, "broker1", 1010)
    val brokerTwo = Broker(2, "broker2", 1011)

    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
    val myBrokerChangeListener = new MyBrokerChangeListener(zookeeperClient)

    zookeeperClient.subscribeBrokerChangeListener(myBrokerChangeListener)
    zookeeperClient.registerBroker(broker)
    zookeeperClient.registerBroker(brokerTwo)


    TestUtils.waitUntilTrue(() => {
      myBrokerChangeListener.brokerSize == 2
    }, "waiting for callback", 2000)
  }
}
