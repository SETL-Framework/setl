package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}

import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Evolving
class Stage extends Logging {

  private[this] var _end: Boolean = true

  private[spark] def end: Boolean = _end

  private[spark] def end_=(value: Boolean): Unit = {
    _end = value
  }

  def start: Boolean = if (stageId == 0) true else false

  private[workflow] var stageId: Int = _

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
