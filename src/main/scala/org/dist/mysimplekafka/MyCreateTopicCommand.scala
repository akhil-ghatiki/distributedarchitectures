package org.dist.mysimplekafka

case class PartitionReplicas(partitionId: Int, brokerIds: List[Int])

class MyCreateTopicCommand(zookeeperClient: MyZookeeperClient) {

  def createTopic(topicName: String, partitionCount: Int, replicationFactor: Int) = {

    val brokerIds = zookeeperClient.getAllBrokerIds()
    val partitionReplicas: Set[PartitionReplicas] = assignReplicasToBrokers(brokerIds.toList, partitionCount, replicationFactor)

    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)
  }

  def assignReplicasToBrokers(brokerList: List[Int], partitionCount: Int, replicationFactor: Int): Set[PartitionReplicas] = {
    val partitionReplicaOne = PartitionReplicas(1, List(1))
    val partitionReplicaTwo = PartitionReplicas(2, List(2))
    Set(partitionReplicaOne, partitionReplicaTwo)
  }

}
