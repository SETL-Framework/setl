package com.jcdecaux.datacorp.spark.workflow

case class DAG(nodes: Set[Node], flows: Set[Flow]) {

  def describe(): Unit = {
    println("----------   Nodes Summary  ----------")
    checkEmpty(nodes)
    nodes.toList.sortBy(_.stage).foreach(_.describe())

    println("----------   Flows Summary  ----------")
    checkEmpty(flows)
    flows.toList.sortBy(_.stage).foreach(_.describe())
  }

  private[this] def checkEmpty(input: Set[_]): Unit = {
    if (input.isEmpty) println("Empty\n")
  }
}
