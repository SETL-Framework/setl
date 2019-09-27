package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.internal.{HasDescription, HasUUIDRegistry, Logging}
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * Pipeline is a complete data transformation workflow.
  */
@InterfaceStability.Evolving
class Pipeline extends Logging with HasUUIDRegistry with HasDescription {

  private[workflow] var deliverableDispatcher: DeliverableDispatcher = new DeliverableDispatcher
  private[workflow] var stageCounter: Int = 0

  val stages: ArrayBuffer[Stage] = ArrayBuffer[Stage]()
  val pipelineInspector: PipelineInspector = new PipelineInspector(this)

  def setInput(v: Deliverable[_]): this.type = {
    deliverableDispatcher.setDelivery(v)
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
      stages += stage.setStageId(stageCounter)
      stageCounter += 1
    } else {
      throw new AlreadyExistsException("Stage already exists")
    }

    this
  }

  def getStage(id: Int): Stage = stages(id)

  /**
    * Mark the last stage as a NON-end stage
    */
  private[this] def resetEndStage(): Unit = {
    if (stages.nonEmpty) stages.last.end = false
  }

  override def describe(): this.type = {
    inspectPipeline()
    pipelineInspector.describe()
    this
  }

  def run(): this.type = {
    inspectPipeline()
    stages
      .foreach {
        stage =>
          // Describe current stage
          stage.describe()

          // Dispatch input if stageID doesn't equal 0
          if (deliverableDispatcher.deliveries.nonEmpty) {
            stage.factories.foreach(deliverableDispatcher.dispatch)
          }

          // run the stage
          stage.run()
          stage.factories.foreach(deliverableDispatcher.collectDeliverable)
      }

    this
  }

  /**
    * Inspect the current pipeline if it has not been inspected
    */
  private[this] def inspectPipeline(): Unit = if (!pipelineInspector.inspected) forceInspectPipeline()

  /**
    * Inspect the current pipeline
    */
  private[this] def forceInspectPipeline(): Unit = {
    pipelineInspector.inspect()
    deliverableDispatcher.setDataFlowGraph(pipelineInspector.getDataFlowGraph)
  }


  /**
    * Get the output of the last factory of the last stage
    *
    * @return an object. it has to be convert to the according type manually.
    */
  def getLastOutput(): Any = {
    stages.last.factories.last.get()
  }

  /**
    * Get the output of a specific Factory
    *
    * @param cls class of the Factory
    * @return
    */
  def getOutput[A](cls: Class[_ <: Factory[_]]): A = {
    val factory = stages.flatMap(s => s.factories).find(f => f.getClass == cls)

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
  def getDeliverable(t: ru.Type): Array[Deliverable[_]] = deliverableDispatcher.findDeliverableByType(t)


}