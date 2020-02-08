package org.dist.mysimplekafka

import org.scalatest.FunSuite

class MyPartitionTest extends FunSuite {

  test("should write and read messages in partition") {
    val partition = new MyPartition()

    partition.append("key1", "message1")
    partition.append("key2", "message2")
    val messages: Seq[partition.Row] = partition.read()

    assert(messages.size == 2)
  }

}
