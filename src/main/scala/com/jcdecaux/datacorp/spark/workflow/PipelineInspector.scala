package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{DeliverySetterMetadata, FactoryOutput, Logging}

@InterfaceStability.Evolving
private[workflow] class PipelineInspector(val pipeline: Pipeline) extends Logging {

  var nodes: Set[Node] = _
  var flows: Set[Flow] = _

  def findNode(classInfo: Class[_]): Option[Node] = nodes.find(_.classInfo == classInfo)

  def createNodes(): Set[Node] = {
    pipeline.stages
      .flatMap(s => s.factories.map({
        f =>
          val inputs = DeliverySetterMetadata.builder()
            .setClass(f.getClass)
            .getOrCreate()
            .flatMap(_.getFactoryInputs) // convert all the Types to String
            .toList

          val output = FactoryOutput(
            outputType = f.deliveryType(),
            consumer = f.consumers
          )
          Node(f.getClass, s.stageId, inputs, output)
      }))
      .toSet
  }

  def createFlows(): Set[Flow] = {
    pipeline
      .stages
      .flatMap({
        stage =>
          val factoriesOfStage = stage.factories

          if (stage.end) {
            Set[Flow]()
          } else {
            factoriesOfStage
              .flatMap({
                f =>
                  val thisNode = findNode(f.getClass).get
                  val payload = f.deliveryType().toString
                  val targetNodes = nodes.filter(n => thisNode.targetNode(n))

                  targetNodes.map(tn => Flow(payload, thisNode, tn, stage.stageId))

              })
              .toSet
          }
      })
      .toSet
  }

  def inspect(): this.type = {
    nodes = createNodes()
    flows = createFlows()
    this
  }

  def describe(): this.type = {
    println("========== Pipeline Summary ==========\n")

    println("----------   Nodes Summary  ----------")
    if (nodes.nonEmpty) {
      nodes.toList.sortBy(_.stage).foreach(_.describe())
    } else {
      println("None")
      println("--------------------------------------")
    }

    println("----------   Flows Summary  ----------")
    if (flows.nonEmpty) {
      flows.toList.sortBy(_.stage).foreach(_.describe())
    } else {
      println("None")
      println("--------------------------------------")
    }

    this
  }
}
