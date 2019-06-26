package com.jcdecaux.datacorp.spark.workflow.v2

case class Node(id: String) {

  def describe(): Unit = {
    println(s"Node: $id")
    println("--------------")

  }
}
