package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Logging

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Unstable
class PipelineOptimizer(val executionPlan: DAG,
                        val parallelism: Int = 4) extends Logging {

  val nodes: Set[Node] = executionPlan.nodes
  val flows: Set[Flow] = executionPlan.flows
  lazy val optimizedDag: DAG = optimize()


  def optimize(): DAG = {
    val nodes = executionPlan.nodes.toList.sortBy(_.stage)
    val oldDag = executionPlan.copy()
    nodes.foldLeft[DAG](oldDag) {
      case (dag, node) => updateNode(node, dag)
    }
  }

  def optimize(stages: ArrayBuffer[Stage]): Array[Stage] = {

    val factories = stages.flatMap(_.factories)

    optimizedDag.nodes.groupBy(_.stage).map {
      case (id, nodes) =>
        val stage = new Stage().setStageId(id)

        val factoryUUIDs = nodes.map(_.factoryUUID)
        factories
          .filter(f => factoryUUIDs.contains(f.getUUID))
          .foreach(stage.addFactory)
        stage
    }.toArray.sortBy(_.stageId)

  }

  private[this] def flowsOf(node: Node, dag: DAG): Set[Flow] = {
    dag.flows.filter(_.to.factoryUUID == node.factoryUUID)
  }

  def updateDag(newNode: Node, dag: DAG): DAG = {
    log.debug(s"Update DAG for node ${newNode.getPrettyName}")
    val oldNode = dag.nodes.find(_.factoryUUID == newNode.factoryUUID).get

    val startingFlows = dag.flows
      .filter(_.from == oldNode)
      .map {
        f => f.copy(stage = newNode.stage, from = newNode)
      }
    val endingFlows = dag.flows
      .filter(_.to == oldNode)
      .map(f => f.copy(to = newNode))

    val otherFlows = dag.flows.filter(f => f.from != oldNode || f.to != oldNode)
    val otherNodes = dag.nodes.filter(_ != oldNode)

    DAG(otherNodes + newNode, startingFlows ++ endingFlows ++ otherFlows)
  }

  @tailrec
  private[this] def validateStage(newStageID: Int, dag: DAG): Int = {
    val nodeCount = dag.nodes.count(_.stage == newStageID)
    if (nodeCount < parallelism) {
      log.debug(s"Valid stage ID: $newStageID")
      newStageID
    } else {
      validateStage(newStageID + 1, dag)
    }
  }

  private[this] def updateNode(oldNode: Node, dag: DAG): DAG = {
    log.debug(s"Optimize node: ${oldNode.getPrettyName} of stage ${oldNode.stage}")
    val currentDag = dag.copy()
    val flows = flowsOf(oldNode, dag)
    val maxInputStage = flows.map(_.stage).max + 1
    log.debug(s"Max input stage of ${oldNode.getPrettyName}: ${maxInputStage}")

    val validStage = validateStage(maxInputStage, dag)

    val newNode = oldNode.copy(stage = validStage)

    updateDag(newNode, currentDag)
  }


}
