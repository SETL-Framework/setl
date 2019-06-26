package com.jcdecaux.datacorp.spark.workflow.v2

case class Edge(payload: String, from: Node, to: Node, stage: Int) {
  def describe(): Unit = {
    println("Edge")
    println(s"Stage $stage")
    println(s"from ${from.id}")
    println(s"to ${to.id}")
    println(s"payLoad $payload")
    println("--------------")
  }
}
