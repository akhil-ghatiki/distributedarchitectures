package org.dist.mysimplekafka

import org.dist.queue.utils.ZkUtils.Broker

case class MyUpdateMetadataRequest(aliveBrokers:List[Broker], leaderReplicas:List[MyLeaderAndReplicas])
