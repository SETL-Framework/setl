package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.BenchmarkResult
import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.exception.AlreadyExistsException
import com.jcdecaux.setl.internal.{HasBenchmark, HasDescription, HasUUIDRegistry, Identifiable, Logging}
import com.jcdecaux.setl.transformation.{Deliverable, Factory}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
 * Pipeline is a complete data transformation workflow.
 */
@InterfaceStability.Evolving
class Pipeline extends Logging with HasUUIDRegistry with HasDescription with Identifiable with HasBenchmark {

  private[this] var _stageCounter: Int = 0
  private[this] var _executionPlan: DAG = _
  private[this] val _deliverableDispatcher: DeliverableDispatcher = new DeliverableDispatcher
  private[this] val _stages: ArrayBuffer[Stage] = ArrayBuffer[Stage]()
  private[this] val _pipelineInspector: PipelineInspector = new PipelineInspector(this)
  private[this] var _pipelineOptimizer: Option[PipelineOptimizer] = None
  private[this] var _benchmarkResult: Array[BenchmarkResult] = Array.empty

  def stages: Array[Stage] = this._stages.toArray

  /**
   * Get the inspector of this pipeline
   *
   * @return
   */
  def pipelineInspector: PipelineInspector = this._pipelineInspector

  /**
   * Get the deliverable dispatcher of this pipeline
   *
   * @return
   */
  def deliverableDispatcher: DeliverableDispatcher = this._deliverableDispatcher

  /**
   * Boolean indicating if the pipeline will be optimized
   *
   * @return
   */
  def optimization: Boolean = _pipelineOptimizer match {
    case Some(_) => true
    case _ => false
  }

  /**
   * Optimise execution with an optimizer
   *
   * @param optimizer an implementation of pipeline optimizer
   * @return this pipeline
   */
  def optimization(optimizer: PipelineOptimizer): this.type = {
    this._pipelineOptimizer = Option(optimizer)
    this
  }

  /**
   * Set to true to allow auto optimization of this pipeline. The default SimplePipelineOptimizer will be used.
   *
   * @param boolean true to allow optimization, otherwise false
   * @return this pipeline
   */
  def optimization(boolean: Boolean): this.type = {
    if (boolean) {
      optimization(new SimplePipelineOptimizer())
    } else {
      optimization(null)
    }
    this
  }

  /**
   * Put the Deliverable to the input pool of the pipeline
   *
   * @param deliverable Deliverable object
   * @return
   */
  def setInput(deliverable: Deliverable[_]): this.type = {
    _deliverableDispatcher.addDeliverable(deliverable)
    this
  }

  /**
   * Set input of the pipeline
   *
   * @param payload    object to be delivered
   * @param consumer   consumer of this payload
   * @param deliveryId id of this delivery
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T,
                              consumer: Seq[Class[_ <: Factory[_]]],
                              deliveryId: String): this.type = {
    val deliverable = new Deliverable[T](payload).setDeliveryId(deliveryId).setConsumers(consumer)
    setInput(deliverable)
  }

  /**
   * Set input of the pipeline
   *
   * @param payload    object to be delivered
   * @param consumer   consumer of this payload
   * @param deliveryId id of this delivery
   * @param consumers  other consumers of the payload
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T,
                              deliveryId: String,
                              consumer: Class[_ <: Factory[_]],
                              consumers: Class[_ <: Factory[_]]*): this.type = {
    setInput(payload, consumer +: consumers, deliveryId)
  }

  /**
   * Set input of the pipeline
   *
   * @param payload   object to be delivered
   * @param consumer  consumer of this payload
   * @param consumers other consumers of the payload
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T,
                              consumer: Class[_ <: Factory[_]],
                              consumers: Class[_ <: Factory[_]]*): this.type = {
    setInput(payload, consumer +: consumers, Deliverable.DEFAULT_ID)
  }

  /**
   * Set input of the pipeline
   *
   * @param payload  object to be delivered
   * @param consumer consumer of this payload
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T,
                              consumer: Class[_ <: Factory[_]]): this.type = {
    setInput[T](payload, List(consumer), Deliverable.DEFAULT_ID)
  }

  /**
   * Set input of the pipeline
   *
   * @param payload    object to be delivered
   * @param consumer   consumer of this payload
   * @param deliveryId id of this delivery
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T,
                              consumer: Class[_ <: Factory[_]],
                              deliveryId: String): this.type = {
    setInput[T](payload, List(consumer), deliveryId)
  }

  /**
   * Set input of the pipeline
   *
   * @param payload object to be delivered
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T): this.type = {
    setInput[T](payload, Seq.empty, Deliverable.DEFAULT_ID)
  }

  /**
   * Set input of the pipeline
   *
   * @param payload    object to be delivered
   * @param deliveryId id of this delivery
   * @tparam T type of payload
   * @return
   */
  def setInput[T: ru.TypeTag](payload: T,
                              deliveryId: String): this.type = {
    setInput[T](payload, Seq.empty, deliveryId)
  }

  /**
   * Add a new stage containing the given factory
   *
   * @param factory Factory to be executed
   * @return
   */
  def addStage(factory: Factory[_]): this.type = addStage(new Stage().addFactory(factory))

  /**
   * Add a new stage containing the given factory
   *
   * @param factory         Factory to be executed
   * @param constructorArgs Arguments of the primary constructor of the factory
   * @throws IllegalArgumentException this will be thrown if arguments don't match
   * @return
   */
  @throws[IllegalArgumentException]("Exception will be thrown if the length of constructor arguments are not correct")
  def addStage(factory: Class[_ <: Factory[_]], constructorArgs: Object*): this.type = {
    addStage(new Stage().addFactory(factory, constructorArgs: _*))
  }

  /**
   * Add a new stage containing the given factory
   *
   * @param constructorArgs Arguments of the primary constructor of the factory
   * @param persistence     if set to true, then the `write` method of the factory will be called to persist the output.
   * @tparam T Class of the Factory
   * @throws IllegalArgumentException this will be thrown if arguments don't match
   * @return
   */
  @throws[IllegalArgumentException]("Exception will be thrown if the length of constructor arguments are not correct")
  def addStage[T <: Factory[_] : ClassTag](constructorArgs: Array[Object] = Array.empty,
                                           persistence: Boolean = true): this.type = {
    addStage(new Stage().addFactory[T](constructorArgs, persistence))
  }

  /**
   * Add a new stage to the pipeline
   *
   * @param stage stage object
   * @return
   */
  def addStage(stage: Stage): this.type = {
    log.debug(s"Add stage ${_stageCounter}")

    if (registerNewItem(stage)) {
      resetEndStage()
      _stages += stage.setStageId(_stageCounter)
      _stageCounter += 1
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
    _executionPlan.describe()
    this
  }

  def run(): this.type = {
    inspectPipeline()
    _benchmarkResult = _stages
      .flatMap {
        stage =>
          // Describe current stage
          stage.describe()

          // dispatch only if deliverable pool is not empty
          if (_deliverableDispatcher.deliverablePool.nonEmpty) {
            stage.factories.foreach(_deliverableDispatcher.dispatch)
          }

          // Disable benchmark if benchmark is false
          this.benchmark match {
            case Some(boo) =>
              if (!boo) {
                log.debug("Disable benchmark")
                stage.benchmark(false)
              }
            case _ =>
          }
          if (!this.benchmark.getOrElse(false)) {
            stage.benchmark(false)
          }

          // run the stage
          stage.run()
          stage.factories.foreach(_deliverableDispatcher.collectDeliverable)

          // retrieve benchmark result
          stage.getBenchmarkResult

      }.toArray

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

    _executionPlan = _pipelineOptimizer match {
      case Some(optimiser) =>
        optimiser.setExecutionPlan(_pipelineInspector.getDataFlowGraph)
        val newStages = optimiser.optimize(_stages)
        this.resetStages(newStages)
        optimiser.getOptimizedExecutionPlan

      case _ => _pipelineInspector.getDataFlowGraph
    }

    _deliverableDispatcher.setDataFlowGraph(_executionPlan)
  }

  private[this] def resetStages(stages: Array[Stage]): Unit = {
    _stages.clear()
    _stages ++= stages
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

  override def getBenchmarkResult: Array[BenchmarkResult] = _benchmarkResult

}