package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{Deliverable, Logging}
import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Unstable
class Stage extends Logging {

  private[workflow] var stageId: Int = 0

  private[workflow] def setStageId(id: Int): this.type = {
    stageId = id
    this
  }

  val factories: ArrayBuffer[Factory[_]] = ArrayBuffer()

  var deliveries: Array[Deliverable[_]] = _

  def addFactory[T <: Factory[_]](factory: T): this.type = {
    factories += factory
    this
  }

  def describe(): this.type = {
    log.info(s"Stage $stageId contains ${factories.length} factories")
    factories.foreach(_.describe())
    this
  }

  def run(): this.type = {
    deliveries = factories
      .par
      .map(_.read().process().write().deliver())
      .toArray
    this
  }

}
