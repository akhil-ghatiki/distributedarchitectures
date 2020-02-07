package org.dist.mysimplekafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging

class MyTopicChangeHandler(myZookeeperClient: MyZookeeperClient, onTopicChange: (String, Seq[MyPartitionReplicas]) => Unit) extends IZkChildListener with Logging {

  var partitionCount = 0

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("change event received")
    val topicName = currentChilds.get(0)
    partitionCount = myZookeeperClient.getPartitionAssignmentsFor(topicName).size
    val replicas: Seq[MyPartitionReplicas] = myZookeeperClient.getPartitionAssignmentsFor(topicName)
    onTopicChange(topicName, replicas)
  }
}
