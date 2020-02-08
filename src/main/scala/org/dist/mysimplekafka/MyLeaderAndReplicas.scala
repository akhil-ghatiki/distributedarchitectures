package org.dist.mysimplekafka

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker

case class MyPartitionInfo(leader:Broker, allReplicas:List[Broker])

case class MyLeaderAndReplicas(topicPartition:TopicAndPartition, partitionStateInfo:MyPartitionInfo)

case class MyLeaderAndReplicaRequest(leaderReplicas:List[MyLeaderAndReplicas])
