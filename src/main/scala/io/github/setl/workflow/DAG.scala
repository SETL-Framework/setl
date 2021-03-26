package io.github.setl.workflow

import io.github.setl.internal.HasDiagram
import io.github.setl.transformation.{Factory, FactoryDeliveryMetadata}
import io.github.setl.util.MermaidUtils

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
   * @return a list of [[io.github.setl.transformation.FactoryDeliveryMetadata]]
   */
  def findDeliveryMetadata(factory: Factory[_]): List[FactoryDeliveryMetadata] = {
    nodes.find(n => n.factoryUUID == factory.getUUID).get.setters
  }

  /** Generate the diagram */
  override def toDiagram: String = {
    val nodeDiagrams = nodes.map(_.toDiagram).mkString("\n")
    val flowDiagrams = flows.map(_.toDiagram).mkString("\n")
    val externalNodeDiagrams = flows.filter(_.from.factoryClass == classOf[External])
      .map(_.from.output.toDiagram).mkString("\n")

    s"""classDiagram
       |$nodeDiagrams
       |$externalNodeDiagrams
       |$flowDiagrams
       |""".stripMargin
  }

  /** Display the diagram */
  override def showDiagram(): Unit = MermaidUtils.printMermaid(this.toDiagram)

  /** Get the diagram ID */
  override def diagramId: String = throw new NotImplementedError("DAG doesn't have diagram id")
}
