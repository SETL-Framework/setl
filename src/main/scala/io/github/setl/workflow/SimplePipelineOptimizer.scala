package io.github.setl.workflow

import io.github.setl.annotation.InterfaceStability
import io.github.setl.internal.Logging

import scala.annotation.tailrec

@InterfaceStability.Unstable
class SimplePipelineOptimizer(val parallelism: Int = 4) extends PipelineOptimizer with Logging {

  private[this] var _executionPlan: DAG = _
  lazy val optExecutionPlan: DAG = optimize()

  override def getOptimizedExecutionPlan: DAG = optExecutionPlan

  override def setExecutionPlan(dag: DAG): SimplePipelineOptimizer.this.type = {
    this._executionPlan = dag
    this
  }

  private[this] def optimize(): DAG = {
    val nodes = _executionPlan.nodes.toList.sortBy(_.stage)
    val oldDag = _executionPlan.copy()
    nodes.foldLeft[DAG](oldDag) {
      case (dag, node) => updateNode(node, dag)
    }
  }

  override def optimize(stages: Iterable[Stage]): Array[Stage] = {
    val factories = stages.flatMap(_.factories)

    optExecutionPlan.nodes.groupBy(_.stage).map {
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

  private[this] def updateDag(newNode: Node, dag: DAG): DAG = {
    logDebug(s"Update DAG for node ${newNode.getPrettyName}")
    val oldNode = dag.nodes.find(_.factoryUUID == newNode.factoryUUID).get

    val startingFlows = dag.flows
      .filter(_.from == oldNode)
      .map(_.copy(from = newNode))

    val endingFlows = dag.flows
      .filter(_.to == oldNode)
      .map(_.copy(to = newNode))

    val otherFlows = dag.flows.filter(_.from != oldNode).filter(_.to != oldNode)

    val otherNodes = dag.nodes.filter(_ != oldNode)

    DAG(otherNodes + newNode, startingFlows ++ endingFlows ++ otherFlows)
  }

  @tailrec
  private[this] def validateStage(newStageID: Int, dag: DAG): Int = {
    val nodeCount = dag.nodes.count(_.stage == newStageID)
    if (nodeCount < parallelism) {
      logDebug(s"Valid stage ID: $newStageID")
      newStageID
    } else {
      validateStage(newStageID + 1, dag)
    }
  }

  private[this] def updateNode(oldNode: Node, dag: DAG): DAG = {
    logDebug(s"Optimize node: ${oldNode.getPrettyName} of stage ${oldNode.stage}")
    val currentDag = dag.copy()
    val flows = flowsOf(oldNode, dag)

    val maxInputStage = flows.size match {
      case 0 => 0
      case _ => flows.map(_.stage).max + 1
    }

    logDebug(s"Max input stage of ${oldNode.getPrettyName}: $maxInputStage")

    val validStage = validateStage(maxInputStage, dag)

    val newNode = oldNode.copy(stage = validStage)

    updateDag(newNode, currentDag)
  }
}
