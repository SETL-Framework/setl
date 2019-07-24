package com.jcdecaux.datacorp.spark.workflow

import scala.reflect.runtime

private[workflow] case class Flow(payload: runtime.universe.Type, from: Node, to: Node, stage: Int) {
  def describe(): Unit = {
    println("Flow")
    println(s"Stage     : $stage")
    println(s"Direction : ${from.getPrettyName} ==> ${to.getPrettyName}")
    println(s"PayLoad   : $payload")
    println("--------------------------------------")
  }
}
