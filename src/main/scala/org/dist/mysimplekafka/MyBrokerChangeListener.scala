package org.dist.mysimplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging

class MyBrokerChangeListener(zookeeperClient: MyZookeeperClient) extends IZkChildListener with Logging {
  var brokerSize = -1;

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("received change event")
    brokerSize = zookeeperClient.getAllBrokerIds().size
  }
}
