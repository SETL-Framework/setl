package com.jcdecaux.datacorp.spark.workflow

import java.util.UUID

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.internal.{Identifiable, Logging}
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray

@InterfaceStability.Evolving
class Stage extends Logging {

  private[this] var _end: Boolean = true

  private[this] var _parallel: Boolean = true

  private[this] var _stageId: Int = _

  val factories: ArrayBuffer[Factory[_]] = ArrayBuffer()

  var deliveries: Array[Deliverable[_]] = _

  private[workflow] def end: Boolean = _end

  private[workflow] def end_=(value: Boolean): Unit = {
    _end = value
  }

  private[workflow] def start: Boolean = if (stageId == 0) true else false

  private[workflow] def stageId: Int = _stageId

  private[workflow] def setStageId(id: Int): this.type = {
    _stageId = id
    this
  }

  def parallel: Boolean = _parallel

  def parallel_=(boo: Boolean): Unit = {
    _parallel = boo
  }

  @throws[IllegalArgumentException]("Exception will be thrown if the length of constructor arguments are not correct")
  def addFactory(factory: Class[_ <: Factory[_]], constructorArgs: Object*): this.type = {

    val primaryConstructor = factory.getConstructors.head

    val newFactory = if (primaryConstructor.getParameterCount == 0) {
      primaryConstructor.newInstance()
    } else {
      primaryConstructor.newInstance(constructorArgs: _*)
    }

    addFactory(newFactory.asInstanceOf[Factory[_]])
  }

  def addFactory(factory: Factory[_]): this.type = {
    factories += factory
    this
  }

  def describe(): this.type = {
    log.info(s"Stage $stageId contains ${factories.length} factories")
    factories.foreach(_.describe())
    this
  }

  def run(): this.type = {
    deliveries = parallelFactories match {
      case Left(par) =>
        log.debug(s"Stage $stageId will be run in parallel mode")
        par.map(_.read().process().write().getDelivery).toArray
      case Right(nonpar) =>
        log.debug(s"Stage $stageId will be run in sequential mode")
        nonpar.map(_.read().process().write().getDelivery)
    }
    this
  }

  private[this] def parallelFactories: Either[ParArray[Factory[_]], Array[Factory[_]]] = {
    if (_parallel) {
      Left(factories.par)
    } else {
      Right(factories.toArray)
    }
  }

}
