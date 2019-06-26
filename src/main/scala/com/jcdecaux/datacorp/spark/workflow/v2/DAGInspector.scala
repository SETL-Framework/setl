package com.jcdecaux.datacorp.spark.workflow.v2

import com.jcdecaux.datacorp.spark.internal.{DispatchManager, Logging}
import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable

class DAGInspector extends Logging {

  val nodes: mutable.HashSet[Node] = mutable.HashSet()
  val edges: mutable.HashSet[Edge] = mutable.HashSet()

  private[this] def getFactoriesFromStage(pipeline: Pipeline, stageId: Int): Array[Factory[_]] = {
    log.debug(s"Find factories from stage $stageId until ${pipeline.stages.length}")
    stageId.until(pipeline.stages.length)
      .flatMap(i => pipeline.getStage(i).factories)
      .toArray
  }

  def describe(): this.type = {
    nodes.foreach(_.describe())
    edges.foreach(_.describe())
    this
  }

  def inspect(pipeline: Pipeline): this.type = {

    pipeline.stages
      .foreach({
        stage =>
          log.debug(s"Inspect stage ${stage.stageId}")
          val factoriesFromNextStages = getFactoriesFromStage(pipeline, stage.stageId + 1)
          log.debug(s"========= ${factoriesFromNextStages.length}")

          stage.factories
            .foreach({
              factory =>

                log.debug(s"Inspect factory ${factory.getClass.getCanonicalName} of stage ${stage.stageId}")

                val nodeId = factory.getClass.getCanonicalName
                val payload: Option[String] = if (stage.end) {
                  log.debug("End stage, no payload")
                  None
                } else {
                  log.debug(s"payload ${factory.deliveryType().toString}")
                  Some(factory.deliveryType().toString)
                }

                val toNodes: Array[Node] = if (stage.end) {
                  log.debug("End stage, no next node")
                  Array[Node]()
                } else {

                  val selected = factoriesFromNextStages
                    .filter({
                      f =>
                        val methodsArgTypes = DispatchManager.getDeliveryAnnotatedMethod(f).map(_._2.map(_.toString)).toArray

                        log.debug(s"method arg types : ${methodsArgTypes.mkString(",   ")}")
                        log.debug(s"fac delivery type ${factory.deliveryType().toString}")
                        log.debug(s"true false ${methodsArgTypes.exists(x => x.contains(factory.deliveryType().toString))}")
                        methodsArgTypes.exists(x => x.contains(factory.deliveryType().toString))
                    })

                  log.debug(s"Find next node(s): ${selected.map(_.getClass.getCanonicalName).mkString(", ")}")

                  selected.map(f => Node(f.getClass.getCanonicalName))
                }

                nodes.add(Node(nodeId))

                toNodes.foreach(x => println(x.id))

                payload match {
                  case Some(pl) => toNodes.foreach(node => edges.add(Edge(pl, Node(nodeId), node, stage.stageId)))
                  case _ =>
                }
            })

      })

    this

  }

}
