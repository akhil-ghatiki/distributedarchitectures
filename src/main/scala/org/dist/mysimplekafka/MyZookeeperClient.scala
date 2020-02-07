package org.dist.mysimplekafka

import com.fasterxml.jackson.core.`type`.TypeReference
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.utils.ZkUtils
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, LeaderAndReplicas}

import scala.jdk.CollectionConverters._

class MyZookeeperClient(zkClient: ZkClient) {
  val BrokerIdsPath = "/brokers/ids"

  val BrokerTopicsPath = "/brokers/topics"
  val ControllerPath = "/controller"
  val ReplicaLeaderElectionPath = "/topics/replica/leader"

  def registerBroker(broker: ZkUtils.Broker) = {
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

  def getPartitionAssignmentsFor(topicName: String): List[MyPartitionReplicas] = {
    val partitionAssignments: String = zkClient.readData(getTopicPath(topicName))
    JsonSerDes.deserialize[List[MyPartitionReplicas]](partitionAssignments.getBytes, new TypeReference[List[MyPartitionReplicas]]() {})
  }

  private def getTopicPath(topicName: String) = {
    BrokerTopicsPath + "/" + topicName
  }

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[MyPartitionReplicas]) = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }

  def createPersistentPath(client: ZkClient, path: String, data: String = ""): Unit = {
    try {
      client.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createPersistent(path, data)
      }
    }
  }

  def subscribeTopicChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerTopicsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  def tryCreatingControllerPath(controllerId: String): Unit = {
    try {
      createEphemeralPath(zkClient, ControllerPath, controllerId)
    } catch {
      case e: ZkNodeExistsException => {
        val existingControllerId: String = zkClient.readData(ControllerPath)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }

  def getPartitionReplicaLeaderInfo(topicName: String): List[LeaderAndReplicas] = {
    val leaderAndReplicas: String = zkClient.readData(getReplicaLeaderElectionPath(topicName))
    JsonSerDes.deserialize[List[LeaderAndReplicas]](leaderAndReplicas.getBytes, new TypeReference[List[LeaderAndReplicas]]() {})
  }

  def getReplicaLeaderElectionPath(topicName: String) = {
    ReplicaLeaderElectionPath + "/" + topicName
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  def setPartitionLeaderForTopic(topicName: String, leaderAndReplicas: List[MyLeaderAndReplicas]): Unit = {

    val leaderReplicaSerializer = JsonSerDes.serialize(leaderAndReplicas)
    val path = getReplicaLeaderElectionPath(topicName);

    try {
      ZkUtils.updatePersistentPath(zkClient, path, leaderReplicaSerializer)
    } catch {
      case e: Throwable => {
        println("Exception while writing data to partition leader data" + e)
      }
    }
  }
}
