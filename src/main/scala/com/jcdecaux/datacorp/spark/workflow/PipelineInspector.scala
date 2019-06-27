package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Logging

@InterfaceStability.Evolving
class PipelineInspector(val pipeline: Pipeline) extends Logging {

  var nodes: Set[Node] = _
  var flows: Set[Flow] = _

  def findNode(id: String): Option[Node] = nodes.find(_.name == id)

  def createNodes(): Set[Node] = {
    pipeline.stages
      .flatMap(s => s.factories.map({
        f =>
          val inputs = DispatchManager
            .getDeliveryAnnotatedMethod(f)
            .flatMap(_._2.map(_.toString)) // convert all the Types to String
            .toArray

          // TODO the current output is a single string, without consumer information, so in the case where
          //  the delivery is for a specific consumer, the graph may be wrong
          val output = f.deliveryType().toString
          Node(f.name, s.stageId, inputs, output)
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
                  val thisNode = findNode(f.name).get
                  val payload = f.deliveryType().toString
                  val targetNodes = nodes.filter(_.input.contains(payload))
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
    println("========== Pipeline Summary ==========")
    println("")
    println("----------   Nodes Summary  ----------")
    nodes.foreach(_.describe())

    println("----------   Flows Summary  ----------")
    flows.foreach(_.describe())
    this
  }
}
