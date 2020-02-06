package org.dist.mysimplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging

class MyTopicChangeHandler(myZookeeperClient: MyZookeeperClient) extends IZkChildListener with Logging{

  var partitionCount = 0

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("change event received")
    partitionCount = myZookeeperClient.getPartitionAssignmentsFor(currentChilds.get(0)).size
  }
}
