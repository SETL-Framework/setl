package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.internal.{HasDescription, HasUUIDRegistry, Identifiable, Logging}
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * Pipeline is a complete data transformation workflow.
  */
@InterfaceStability.Evolving
class Pipeline extends Logging with HasUUIDRegistry with HasDescription with Identifiable {

  private[this] var stageCounter: Int = 0
  private[this] var executionPlan: DAG = _
  private[this] var _optimization: Boolean = false
  private[this] val _deliverableDispatcher: DeliverableDispatcher = new DeliverableDispatcher
  private[this] val _stages: ArrayBuffer[Stage] = ArrayBuffer[Stage]()
  private[this] val _pipelineInspector: PipelineInspector = new PipelineInspector(this)

  def setInput(v: Deliverable[_]): this.type = {
    _deliverableDispatcher.setDelivery(v)
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

  def stages: ArrayBuffer[Stage] = this._stages

  def pipelineInspector: PipelineInspector = this._pipelineInspector

  def deliverableDispatcher: DeliverableDispatcher = this._deliverableDispatcher

  def optimization: Boolean = this._optimization

  /**
    * Set to true to allow auto optimization of this pipeline.
    *
    * @param boolean true to allow optimization, otherwise false
    * @return this pipeline
    */
  def optimization(boolean: Boolean): this.type = {
    _optimization = boolean
    this
  }

  def setInput[T: ru.TypeTag](v: T, consumer: Class[_]): this.type = setInput[T](v, Some(consumer))

  def setInput[T: ru.TypeTag](v: T, consumer: Class[_], consumers: Class[_]*): this.type = {
    val deliverable = new Deliverable[T](v)
    (consumer +: consumers).foreach(c => deliverable.setConsumer(c))
    setInput(deliverable)
  }

  def setInput[T: ru.TypeTag](v: T): this.type = setInput[T](v, None)

  def addStage(factory: Factory[_]): this.type = addStage(new Stage().addFactory(factory))

  @throws[IllegalArgumentException]("Exception will be thrown if the length of constructor arguments are not correct")
  def addStage(factory: Class[_ <: Factory[_]], constructorArgs: Object*): this.type = {
    addStage(new Stage().addFactory(factory, constructorArgs: _*))
  }

  def addStage(stage: Stage): this.type = {
    log.debug(s"Add stage $stageCounter")

    if (registerNewItem(stage)) {
      resetEndStage()
      _stages += stage.setStageId(stageCounter)
      stageCounter += 1
    } else {
      throw new AlreadyExistsException("Stage already exists")
    }

    this
  }

  def getStage(id: Int): Stage = _stages(id)

  /**
    * Mark the last stage as a NON-end stage
    */
  private[this] def resetEndStage(): Unit = {
    if (_stages.nonEmpty) _stages.last.end = false
  }

  override def describe(): this.type = {
    inspectPipeline()
    executionPlan.describe()
    this
  }

  def run(): this.type = {
    inspectPipeline()
    _stages
      .foreach {
        stage =>
          // Describe current stage
          stage.describe()

          // Dispatch input if stageID doesn't equal 0
          if (_deliverableDispatcher.deliveries.nonEmpty) {
            stage.factories.foreach(_deliverableDispatcher.dispatch)
          }

          // run the stage
          stage.run()
          stage.factories.foreach(_deliverableDispatcher.collectDeliverable)
      }

    this
  }

  /**
    * Inspect the current pipeline if it has not been inspected
    */
  private[this] def inspectPipeline(): Unit = if (!_pipelineInspector.inspected) forceInspectPipeline()

  /**
    * Inspect the current pipeline
    */
  private[this] def forceInspectPipeline(): Unit = {
    _pipelineInspector.inspect()

    val optimiser = new PipelineOptimizer(_pipelineInspector.getDataFlowGraph)

    executionPlan = if (_optimization) {
      val newStages = optimiser.optimize(_stages)
      _stages.clear()
      _stages ++= newStages
      optimiser.optimizedDag

    } else {
      _pipelineInspector.getDataFlowGraph
    }

    _deliverableDispatcher.setDataFlowGraph(executionPlan)
  }


  /**
    * Get the output of the last factory of the last stage
    *
    * @return an object. it has to be convert to the according type manually.
    */
  def getLastOutput: Any = {
    _stages.last.factories.last.get()
  }

  /**
    * Get the output of a specific Factory
    *
    * @param cls class of the Factory
    * @return
    */
  def getOutput[A](cls: Class[_ <: Factory[_]]): A = {
    val factory = _stages.flatMap(s => s.factories).find(f => f.getClass == cls)

    factory match {
      case Some(x) => x.get().asInstanceOf[A]
      case _ => throw new NoSuchElementException(s"There isn't any class ${cls.getCanonicalName}")
    }
  }

  /**
    * Get the Deliverable of the given runtime Type
    *
    * @param t runtime type of the Deliverable's payload
    * @return
    */
  def getDeliverable(t: ru.Type): Array[Deliverable[_]] = _deliverableDispatcher.findDeliverableByType(t)


}