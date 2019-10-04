package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{HasDescription, Logging}
import com.jcdecaux.datacorp.spark.transformation.Factory

/**
  * PipelineInspector will inspect a given [[com.jcdecaux.datacorp.spark.workflow.Pipeline]] and create a
  * Directed Acyclic Graph (DAG) with nodes (factories) and flows (data transfer flows)
  *
  * @param pipeline an instantiated pipeline
  */
@InterfaceStability.Evolving
private[workflow] class PipelineInspector(val pipeline: Pipeline) extends Logging with HasDescription {

  private[workflow] var nodes: Set[Node] = _
  private[workflow] var flows: Set[Flow] = _

  /**
    * Get a Directed Acyclic Graph from the given pipeline.
    *
    * @return a DAG object if the pipeline is already inspected, otherwise null
    */
  def getDataFlowGraph: DAG = {
    require(nodes != null)
    require(flows != null)
    DAG(nodes, flows)
  }

  /**
    * Return true if the input pipeline is already inspected. False otherwise
    *
    * @return boolean
    */
  def inspected: Boolean = if (nodes == null || flows == null) false else true

  /**
    * Find the corresponding node of a factory in the pool
    *
    * @param factory a Factory object
    * @return an option of Node
    */
  def findNode(factory: Factory[_]): Option[Node] = nodes.find(_.factoryUUID == factory.getUUID)

  private[this] def createNodes(): Set[Node] = {
    pipeline
      .stages
      .flatMap(stage => stage.createDAGNodes())
      .toSet
  }

  private[this] def createInternalFlows(): Set[Flow] = {
    pipeline
      .stages
      .flatMap {
        stage =>
          val factoriesOfStage = stage.factories

          if (stage.end) {
            Set[Flow]()
          } else {
            factoriesOfStage
              .flatMap {
                f =>
                  val thisNode = findNode(f).get
                  val payloadType = f.deliveryType()
                  val targetNodes = nodes
                    .filter(n => n.stage > thisNode.stage)
                    .filter(n => thisNode.targetNode(n))

                  targetNodes.map(tn => Flow(payloadType, thisNode, tn, stage.stageId))
              }
              .toSet
          }
      }
      .toSet
  }

  private[this] def createExternalFlows(internalFlows: Set[Flow]): Set[Flow] = {
    require(nodes != null)

    nodes
      .flatMap {
        thisNode =>
          thisNode.input
            .filter(_.producer == classOf[External])
            .map(nodeInput => Flow(nodeInput.runtimeType, External.NODE, thisNode, thisNode.stage))
            .filter(thisFlow => !internalFlows.exists(f => f.payload == thisFlow.payload && f.to == thisNode))
      }
  }

  private[this] def createFlows(): Set[Flow] = {
    val internalFlows = createInternalFlows()
    val externalFlows = createExternalFlows(internalFlows)
    internalFlows ++ externalFlows
  }

  /**
    * Inspect the pipeline and generate the corresponding flows and nodes
    *
    * @return
    */
  def inspect(): this.type = {
    nodes = createNodes()
    flows = createFlows()
    this
  }

  override def describe(): this.type = {
    println("========== Pipeline Summary ==========\n")
    getDataFlowGraph.describe()
    this
  }
}
