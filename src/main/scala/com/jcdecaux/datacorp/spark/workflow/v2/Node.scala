package com.jcdecaux.datacorp.spark.workflow.v2

case class Node(name: String, stage: Int, input: Array[String], output: String) {

  def getName: String = {
    name.split("\\.").last
  }

  def describe(): Unit = {
    println(s"Node   : $getName")
    println(s"Stage  : $stage")
    input.foreach(i => println(s"Input  : $i"))
    println(s"Output : $output") //
    println("--------------------------------------")
  }
}
