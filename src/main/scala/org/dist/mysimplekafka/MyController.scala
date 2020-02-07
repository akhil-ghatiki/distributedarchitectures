package org.dist.mysimplekafka

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka._

import scala.jdk.CollectionConverters._

class MyController(zookeeperClient: MyZookeeperClient, val brokerId: Int) {
  val correlationId = new AtomicInteger(0)
  var currentLeader = -1
  var name = ""
  var liveBrokers: Set[Broker] = Set()

  def startUp() = {
    elect()
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  private def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    val myTopicChangeHandler = new MyTopicChangeHandler(zookeeperClient, onTopicChange)
    zookeeperClient.subscribeTopicChangeListener(myTopicChangeHandler)
  }

  def onTopicChange(topicName: String, partitionReplicas: Seq[MyPartitionReplicas]) = {
    val leaderAndReplicas: Seq[MyLeaderAndReplicas] = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)

    zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicas.toList);
    //This is persisted in zookeeper for failover.. we are just keeping it in memory for now.
    sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
    sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas)

  }

  private def sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas: Seq[MyLeaderAndReplicas]) = {
    val brokerListToIsrRequestMap =
      liveBrokers.foreach(broker ⇒ {
        val updateMetadataRequest = MyUpdateMetadataRequest(liveBrokers.toList, leaderAndReplicas.toList)
        val request = RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest), correlationId.incrementAndGet())
        //        socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
      })
  }


  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[MyPartitionReplicas]) = {
    val leaderAndReplicas: Seq[MyLeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      MyLeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), MyPartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }

  private def getBroker(brokerId: Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }

  @VisibleForTesting
  def sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas: Seq[MyLeaderAndReplicas], partitionReplicas: Seq[MyPartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[MyLeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[MyLeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for (broker ← brokers) {
      val leaderAndReplicas: java.util.List[MyLeaderAndReplicas] = brokerToLeaderIsrRequest.get(broker)
      val leaderAndReplicaRequest = MyLeaderAndReplicaRequest(leaderAndReplicas.asScala.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), correlationId.getAndIncrement())
      //socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

}
