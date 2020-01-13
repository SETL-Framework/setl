package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.internal.HasDiagram
import com.jcdecaux.setl.transformation.{Factory, FactoryDeliveryMetadata}

private[workflow] case class DAG(nodes: Set[Node], flows: Set[Flow]) extends HasDiagram {

  def describe(): Unit = {
    println("-------------   Data Transformation Summary  -------------")
    checkEmpty(nodes)
    nodes.toList.sortBy(_.stage).foreach(_.describe())

    println("------------------   Data Flow Summary  ------------------")
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
   * @return a list of [[com.jcdecaux.setl.transformation.FactoryDeliveryMetadata]]
   */
  def findDeliveryMetadata(factory: Factory[_]): List[FactoryDeliveryMetadata] = {
    nodes.find(n => n.factoryUUID == factory.getUUID).get.setters
  }

  override def toDiagram: String = {

    val nodeDiagrams = nodes.map(_.toDiagram).mkString("\n")
    val flowDiagrams = flows.map(_.toDiagram).mkString("\n")

    s"""classDiagram
       |$nodeDiagrams
       |$flowDiagrams
       |""".stripMargin
  }

  override def diagramId: String = throw new NotImplementedError("DAG doesn't have diagram id")
}
