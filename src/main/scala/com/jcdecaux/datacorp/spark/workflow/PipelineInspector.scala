package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{HasDescription, Logging}
import com.jcdecaux.datacorp.spark.transformation.{Factory, FactoryDeliveryMetadata, FactoryOutput}

import scala.collection.mutable

/**
  * PipelineInspector will inspect a given [[com.jcdecaux.datacorp.spark.workflow.Pipeline]] and create a DAG with
  * nodes (factories) and flows (data transfer flows)
  *
  * @param pipeline an instantiated pipeline
  */
@InterfaceStability.Evolving
private[workflow] class PipelineInspector(val pipeline: Pipeline) extends Logging with HasDescription {

  private[workflow] var nodes: Set[Node] = _
  private[workflow] var flows: Set[Flow] = _
  private[workflow] var setters: mutable.HashSet[FactoryDeliveryMetadata] = mutable.HashSet()

  def dag: DAG = DAG(nodes, flows)

  /**
    * Return true if the input pipeline is already inspected. False otherwise
    *
    * @return boolean
    */
  def inspected: Boolean = if (nodes == null || flows == null) false else true

  /**
    * Find all the setter methods of the given Factory
    *
    * @param factory an instantiated Factory
    * @return a list of [[com.jcdecaux.datacorp.spark.transformation.FactoryDeliveryMetadata]]
    */
  def findSetters(factory: Factory[_]): List[FactoryDeliveryMetadata] = {
    require(inspected)
    setters.filter(s => s.factoryUUID == factory.getUUID).toList
  }

  /**
    * Find the corresponding node of a factory in the pool
    *
    * @param factory a Factory object
    * @return an option of Node
    */
  def findNode(factory: Factory[_]): Option[Node] = nodes.find(_.factoryUUID == factory.getUUID)

  private[this] def createNodes(): Set[Node] = {
    pipeline.stages
      .flatMap {
        stage =>
          stage.factories.map {
            fac =>
              val setter = FactoryDeliveryMetadata.builder().setFactory(fac).getOrCreate()
              setters ++= setter

              val inputs = setter.flatMap(_.getFactoryInputs).toList
              val output = FactoryOutput(runtimeType = fac.deliveryType(), consumer = fac.consumers)

              Node(fac.getClass, fac.getUUID, stage.stageId, inputs, output)
          }
      }
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
            .map(nodeInput => Flow(nodeInput.runtimeType, External, thisNode, thisNode.stage))
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

  override def describe(): this.type = {
    println("========== Pipeline Summary ==========\n")
    dag.describe()
    this
  }
}
