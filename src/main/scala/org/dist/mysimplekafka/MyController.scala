package org.dist.mysimplekafka

import org.dist.simplekafka.ControllerExistsException

class MyController(zookeeperClient: MyZookeeperClient, brokerId : Int) {
  var currentLeader = -1

  def startUp() = {
    elect()
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

}
