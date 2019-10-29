package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.{AlreadyExistsException, InvalidDeliveryException}
import com.jcdecaux.datacorp.spark.internal.{HasUUIDRegistry, Logging}
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory, FactoryDeliveryMetadata}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * DeliverableDispatcher use a Directed Acyclic Graph (DAG) to transfer data between different stages.
  *
  * It can:
  * <ul>
  * <li>collect a [[com.jcdecaux.datacorp.spark.transformation.Deliverable]]
  * from a [[com.jcdecaux.datacorp.spark.transformation.Factory]]</li>
  * <li>find the right [[com.jcdecaux.datacorp.spark.transformation.Deliverable]]
  * from its pool and send it to a [[com.jcdecaux.datacorp.spark.transformation.Factory]]</li>
  * </ul>
  */
@InterfaceStability.Evolving
private[spark] class DeliverableDispatcher extends Logging with HasUUIDRegistry {

  private[workflow] val deliveries: ArrayBuffer[Deliverable[_]] = ArrayBuffer()
  private[this] var graph: DAG = _

  def setDataFlowGraph(graph: DAG): this.type = {
    this.graph = graph
    this
  }

  /**
    * Add a new Deliverable object into DeliverableDispatcher's delivery pool. <br>
    *
    * <b>Attention:</b> DeliverableDispatcher only guarantee that each deliverable object has different UUID.
    * In other word, use cannot call twice <code>setDelivery</code> with the same argument. However, it doesn't
    * check if multiple deliverables have the same data type with the same producer and the same consumer
    *
    * @param v : Deliverable
    * @return this DeliverableDispatcher
    */
  private[workflow] def setDelivery(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery: ${v.payloadType}. Producer: ${v.producer}")

    if (registerNewItem(v)) {
      deliveries.append(v)
    } else {
      throw new AlreadyExistsException(s"The current deliverable ${v.getUUID} already exists")
    }
    this
  }

  /**
    * Find the corresponding [[Deliverable]] from the pool with the given runtime Type information
    *
    * @param deliveryType runtime type
    * @return
    */
  @throws[InvalidDeliveryException]("find multiple matched deliveries")
  private[this] def getDelivery(deliveryType: ru.Type,
                                consumer: Class[_ <: Factory[_]],
                                producer: Class[_ <: Factory[_]]): Option[Deliverable[_]] = {

    val availableDeliverable = findDeliverableByType(deliveryType)

    availableDeliverable.length match {
      case 0 =>
        log.warn("No deliverable available")
        None
      case 1 =>

        val deliverable = availableDeliverable.head

        if (deliverable.consumer.nonEmpty && !deliverable.consumer.contains(consumer)) {
          throw new InvalidDeliveryException(s"Can't find ${consumer.getSimpleName} in the consumer list")
        }

        log.debug("Find Deliverable")
        Some(availableDeliverable.head)
      case _ =>
        log.info("Multiple Deliverable with same type were received. Try matching by producer")
        val matchedByProducer = availableDeliverable.filter(_.producer == producer)

        matchedByProducer.length match {
          case 0 =>
            log.warn("No deliverable available")
            None
          case 1 =>
            log.debug("Find Deliverable")
            Some(matchedByProducer.head)
          case _ =>
            log.info("Multiple Deliverable with same type and same producer were received. Try matching by consumer")
            val matchedByConsumer = matchedByProducer
              .filter(_.consumer.nonEmpty)
              .filter(_.consumer.contains(consumer))

            matchedByConsumer.length match {
              case 0 =>
                log.warn("No deliverable available")
                None
              case 1 =>
                log.debug("Find Deliverable")
                Some(matchedByConsumer.head)
              case _ =>
                throw new InvalidDeliveryException("Find multiple deliveries having the same type, producer and consumer")
            }
        }
    }
  }

  /**
    * Get all the [[Deliverable]] by matching their payload's type with the given type
    *
    * @param deliveryType type of data
    * @return
    */
  private[workflow] def findDeliverableByType(deliveryType: ru.Type): Array[Deliverable[_]] = {
    deliveries.filter(_.hasSamePayloadType(deliveryType)).toArray
  }

  /**
    * Collect a [[Deliverable]] from a [[Factory]]
    *
    * @param factory a factory object
    * @return
    */
  private[workflow] def collectDeliverable(factory: Factory[_]): this.type = {
    setDelivery(factory.getDelivery)
  }

  private[workflow] def collectDeliverable(stage: Stage): this.type = {
    stage.deliveries.foreach(setDelivery)
    this
  }

  /**
    * Used only for testing
    */
  private[workflow] def _dispatch(factory: Factory[_]): this.type = {
    val setters = FactoryDeliveryMetadata
      .builder()
      .setFactory(factory)
      .getOrCreate()

    dispatch(factory, setters)
  }

  /**
    * For the given factory, find its setters from the DAG and dispatch deliveries
    *
    * @param factory a Factory[A] object
    * @return
    */
  @throws[NoSuchElementException]("Cannot find any matched delivery")
  @throws[InvalidDeliveryException]("Find multiple matched deliveries")
  private[workflow] def dispatch(factory: Factory[_]): this.type = {
    val setters = this.graph.findSetters(factory)
    dispatch(factory, setters)
  }

  /**
    * Dispatch the right deliverable object to the corresponding methods
    * (denoted by the @Delivery annotation) of a factory
    *
    * @param factory target factory
    * @return
    */
  private[this] def dispatch(factory: Factory[_], setters: Iterable[FactoryDeliveryMetadata]): this.type = {
    setters
      .foreach {
        setterMethod =>
          // Loop through the type of all arguments of a method and get the Deliverable that correspond to the type
          val args = setterMethod.argTypes.map {
            argType =>
              log.debug(s"Dispatch ${simpleTypeNameOf(argType)} by calling " +
                s"${factory.getClass.getCanonicalName}.${setterMethod.name}")

              getDelivery(
                deliveryType = argType,
                consumer = factory.getClass,
                producer = setterMethod.producer
              ) match {
                case Some(delivery) => delivery
                case _ =>
                  if (!setterMethod.optional) {
                    throw new NoSuchElementException(s"Can not find type $argType from DispatchManager")
                  } else {
                    Deliverable.empty()
                  }
              }
          }

          if (args.exists(_.isEmpty)) {
            log.warn(s"No deliverable was found for optional input ${setterMethod.name}")
          } else {
            val factorySetter = factory.getClass.getMethod(setterMethod.name, args.map(_.classInfo): _*)
            factorySetter.invoke(factory, args.map(_.get.asInstanceOf[Object]): _*)
          }
      }
    this
  }

  private[this] val simpleTypeNameOf: ru.Type => String = {
    rType => rType.toString.split("\\[").map(_.split("\\.").last).mkString("[")
  }

}
