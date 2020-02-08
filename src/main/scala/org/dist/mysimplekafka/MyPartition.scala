package org.dist.mysimplekafka

class MyPartition() {

  def read(): Seq[Row] = {
    null
  }

  def append(key: String, message: String): Unit = {

  }

  case class Row(key: String, value: String)
}
