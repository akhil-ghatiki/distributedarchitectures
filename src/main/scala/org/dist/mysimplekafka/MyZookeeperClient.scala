package org.dist.mysimplekafka

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.utils.ZkUtils

import scala.jdk.CollectionConverters._

class MyZookeeperClient(zkClient: ZkClient) {

  val BrokerIdsPath = "/brokers/ids"

  def registerBroker(broker: ZkUtils.Broker)= {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }
  def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toSet
  }
  private def getBrokerPath(id: Int) = {
    BrokerIdsPath + "/" + id
  }
  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }
  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }
}
