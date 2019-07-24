package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{DeliverySetterMetadata, FactoryOutput, Logging}

@InterfaceStability.Evolving
private[workflow] class PipelineInspector(val pipeline: Pipeline) extends Logging {

  var nodes: Set[Node] = _
  var flows: Set[Flow] = _

  def inspected: Boolean = if (nodes == null || flows == null) false else true

  def findNode(classInfo: Class[_]): Option[Node] = nodes.find(_.classInfo == classInfo)

  private[this] def createNodes(): Set[Node] = {
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

          Node(f.getClass, f.getUUID, s.stageId, inputs, output)
      }))
      .toSet
  }

  private[this] def createInternalFlows(): Set[Flow] = {
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
                  val payloadType = f.deliveryType()
                  val targetNodes = nodes.filter(n => thisNode.targetNode(n))

                  targetNodes.map(tn => Flow(payloadType, thisNode, tn, stage.stageId))
              })
              .toSet
          }
      })
      .toSet
  }

  private[this] def createExternalFlows(internalFlows: Set[Flow]): Set[Flow] = {
    require(nodes != null)

    nodes
      .flatMap {
        thisNode =>
          thisNode.input
            .filter(_.producer == classOf[External])
            .map(nd => Flow(nd.inputType, External, thisNode, thisNode.stage))
            .filter(thisFlow => !internalFlows.exists(f => f.payload == thisFlow.payload && f.to == thisNode))
      }
  }

  private[this] def createFlows(): Set[Flow] = {
    val internalFlows = createInternalFlows()
    val externalFlows = createExternalFlows(internalFlows)
    internalFlows ++ externalFlows
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
