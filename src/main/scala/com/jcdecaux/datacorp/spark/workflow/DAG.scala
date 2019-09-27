package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.transformation.{Factory, FactoryDeliveryMetadata}

private[workflow] case class DAG(nodes: Set[Node], flows: Set[Flow]) {

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

  /**
    * Find all the setter methods of the given Factory
    *
    * @param factory an instantiated Factory
    * @return a list of [[com.jcdecaux.datacorp.spark.transformation.FactoryDeliveryMetadata]]
    */
  def findSetters(factory: Factory[_]): List[FactoryDeliveryMetadata] = {
    nodes.find(n => n.factoryUUID == factory.getUUID).get.setters
  }
}
