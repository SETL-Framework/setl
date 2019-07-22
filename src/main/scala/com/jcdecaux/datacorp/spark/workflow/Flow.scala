package com.jcdecaux.datacorp.spark.workflow

private[workflow] case class Flow(payload: String, from: Node, to: Node, stage: Int) {
  def describe(): Unit = {
    println("Flow")
    println(s"Stage     : $stage")
    println(s"Direction : ${from.getPrettyName} ==> ${to.getPrettyName}")
    println(s"PayLoad   : $payload")
    println("--------------------------------------")
  }
}
