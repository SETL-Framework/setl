package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.internal.{HasDescription, HasUUIDRegistry, Identifiable, Logging}
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory, FactoryDeliveryMetadata, FactoryOutput}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag

/**
 * A Stage is a collection of independent Factories. All the stages of a pipeline will be executed
 * sequentially at runtime. Within a stage, all factories could be executed in parallel or in sequential order.
 */
@InterfaceStability.Evolving
class Stage extends Logging with Identifiable with HasUUIDRegistry with HasDescription {

  private[this] var _optimization: Boolean = false
  private[this] var _end: Boolean = true
  private[this] var _parallel: Boolean = true
  private[this] var _stageId: Int = _
  private[this] val _factories: ArrayBuffer[Factory[_]] = ArrayBuffer()
  private[this] var _deliverable: Array[Deliverable[_]] = _
  private[this] var _persistence: Boolean = true

  private[workflow] def end: Boolean = _end

  private[workflow] def end_=(value: Boolean): Unit = _end = value

  private[workflow] def start: Boolean = if (stageId == 0) true else false

  private[workflow] def stageId: Int = _stageId

  private[workflow] def setStageId(id: Int): this.type = {
    _stageId = id
    this
  }

  def factories: ArrayBuffer[Factory[_]] = this._factories

  def deliverable: Array[Deliverable[_]] = this._deliverable

  def parallel: Boolean = _parallel

  def persist: Boolean = this._persistence

  def persist(persistence: Boolean): this.type = {
    this._persistence = persistence
    this
  }

  /**
   * Set to true to run all factories of this stage in parallel. Otherwise they will be executed in sequential order
   *
   * @param boo true for parallel. otherwise false
   * @return
   */
  def parallel(boo: Boolean): this.type = {
    _parallel = boo
    this
  }

  def optimization: Boolean = this._optimization

  /**
   * Set to true to allow the PipelineOptimizer to optimize the execution order of factories within the stage. Default
   * false
   *
   * @param boo true to allow optimization
   * @return this stage
   */
  def optimization(boo: Boolean): this.type = {
    _optimization = boo
    this
  }

  private[this] def instantiateFactory(cls: Class[_ <: Factory[_]],
                                       constructorArgs: Array[Object]): Factory[_] = {
    val primaryConstructor = cls.getConstructors.head

    val newFactory = if (primaryConstructor.getParameterCount == 0) {
      primaryConstructor.newInstance()
    } else {
      primaryConstructor.newInstance(constructorArgs: _*)
    }

    newFactory.asInstanceOf[Factory[_]]
  }

  @throws[IllegalArgumentException]("Exception will be thrown if the length of constructor arguments are not correct")
  def addFactory(factory: Class[_ <: Factory[_]], constructorArgs: Object*): this.type = {
    addFactory(instantiateFactory(factory, constructorArgs.toArray))
  }

  @throws[IllegalArgumentException]("Exception will be thrown if the length of constructor arguments are not correct")
  def addFactory[T <: Factory[_] : ClassTag](constructorArgs: Array[Object] = Array.empty,
                                             persistence: Boolean = true): this.type = {
    val cls = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    addFactory(instantiateFactory(cls, constructorArgs).persist(persistence))
  }

  @throws[AlreadyExistsException]
  def addFactory(factory: Factory[_]): this.type = {
    if (registerNewItem(factory)) {
      _factories += factory
    } else {
      throw new AlreadyExistsException(s"The current factory ${factory.getCanonicalName} (${factory.getUUID.toString})" +
        s"already exists")
    }
    this
  }

  override def describe(): this.type = {
    log.info(s"Stage $stageId contains ${_factories.length} factories")
    _factories.foreach(_.describe())
    this
  }

  def run(): this.type = {
    _deliverable = parallelFactories match {
      case Left(par) =>
        log.debug(s"Stage $stageId will be run in parallel mode")
        par.map(runFactory).toArray

      case Right(nonpar) =>
        log.debug(s"Stage $stageId will be run in sequential mode")
        nonpar.map(runFactory)
    }
    this
  }

  private[this] val runFactory: Factory[_] => Deliverable[_] = {
    factory: Factory[_] =>
      factory.read().process()
      if (this.persist && factory.persist) {
        log.debug(s"Persist output of ${factory.getPrettyName}")
        factory.write()
      }
      factory.getDelivery
  }

  private[this] def parallelFactories: Either[ParArray[Factory[_]], Array[Factory[_]]] = {
    if (_parallel) {
      Left(_factories.par)
    } else {
      Right(_factories.toArray)
    }
  }

  private[workflow] def createDAGNodes(): Array[Node] = {

    _factories.map {
      fac =>
        val setter = FactoryDeliveryMetadata.builder().setFactory(fac).getOrCreate()
        val output = FactoryOutput(runtimeType = fac.deliveryType(), consumer = fac.consumers, deliveryId = fac.deliveryId)

        Node(fac.getClass, fac.getUUID, this.stageId, setter.toList, output)
    }.toArray
  }

}
