package com.jcdecaux.datacorp.spark.workflow.v2

import com.jcdecaux.datacorp.spark.internal.{Deliverable, DispatchManager, Logging}
import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

class Pipeline extends Logging {

  var stages: ArrayBuffer[Stage] = ArrayBuffer[Stage]()
  val dispatchManagers: DispatchManager = new DispatchManager

  def setInput(v: Deliverable[_]): this.type = {
    dispatchManagers.setDelivery(v)
    this
  }

  def getOutput(t: ru.Type): Array[Deliverable[_]] = dispatchManagers.getDeliveries(t)

  private[workflow] var stageCounter: Int = 0

  def addStage(factory: Factory[_]): this.type = {
    addStage(new Stage().addFactory(factory))
  }

  def addStage(stage: Stage): this.type = {
    log.debug(s"Add stage $stageCounter")
    stages += stage.setStageId(stageCounter)
    stageCounter += 1
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
            stage.factories.foreach({
              x =>
                dispatchManagers.dispatch(x)
            })
          }

          // run the stage
          stage.run()
          stage.factories.foreach(dispatchManagers.collectDeliverable)
      })

    this
  }


}