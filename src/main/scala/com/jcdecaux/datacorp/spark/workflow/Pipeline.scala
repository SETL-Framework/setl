package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

@InterfaceStability.Evolving
class Pipeline extends Logging {

  private[workflow] val dispatchManagers: DispatchManager = new DispatchManager
  private[workflow] var stageCounter: Int = 0

  var stages: ArrayBuffer[Stage] = ArrayBuffer[Stage]()
  var pipelineInspector: PipelineInspector = _

  def setInput(v: Deliverable[_]): this.type = {
    dispatchManagers.setDelivery(v)
    this
  }

  def setInput[T: ru.TypeTag](v: T, consumer: Option[Class[_]]): this.type = {
    val deliverable = new Deliverable[T](v)

    consumer match {
      case Some(c) => deliverable.setConsumer(c)
      case _ =>
    }

    setInput(deliverable)
  }

  def setInput[T: ru.TypeTag](v: T, consumer: Class[_], consumers: Class[_]*): this.type = {
    val deliverable = new Deliverable[T](v)

    deliverable.setConsumer(consumer)
    consumers.foreach(c => deliverable.setConsumer(c))

    setInput(deliverable)
  }

  def setInput[T: ru.TypeTag](v: T, consumer: Class[_]): this.type = setInput(v, Some(consumer))

  def setInput[T: ru.TypeTag](v: T): this.type = setInput(v, None)

  def getOutput(t: ru.Type): Array[Deliverable[_]] = dispatchManagers.findDeliverableByType(t)

  def addStage(factory: Factory[_]): this.type = addStage(new Stage().addFactory(factory))

  def addStage(stage: Stage): this.type = {
    log.debug(s"Add stage $stageCounter")
    markEndStage()
    stages += stage.setStageId(stageCounter)
    stageCounter += 1
    this
  }

  def getStage(id: Int): Stage = stages(id)

  private[this] def markEndStage(): Unit = {
    if (stages.nonEmpty) {
      stages.last.end = false
    }
  }

  def describe(): this.type = {
    pipelineInspector = new PipelineInspector(this).inspect().describe()
    this
  }

  def run(): this.type = {
    stages
      .foreach({
        stage =>

          // Describe current stage
          stage.describe()

          // Dispatch input if stageID doesn't equal 0
          if (dispatchManagers.deliveries.nonEmpty) {
            stage.factories.foreach(dispatchManagers.dispatch)
          }

          // run the stage
          stage.run()
          stage.factories.foreach(dispatchManagers.collectDeliverable)
      })

    this
  }


}