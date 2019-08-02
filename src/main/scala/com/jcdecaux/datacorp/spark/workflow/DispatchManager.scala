package com.jcdecaux.datacorp.spark.workflow

import java.util.UUID

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.exception.AlreadyExistsException
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory, FactoryDeliveryMetadata}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * DispatchManager will handle the data dispatch between different stages.
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
private[spark] class DispatchManager extends Logging {

  private[this] val deliveryRegister: scala.collection.mutable.HashSet[UUID] = scala.collection.mutable.HashSet()
  private[workflow] val deliveries: ArrayBuffer[Deliverable[_]] = ArrayBuffer()

  private[workflow] def setDelivery(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery: ${v.payloadType}. Producer: ${v.producer}")

    if (deliveryRegister.add(v.getUUID)) {
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
  private[this] def getDelivery(deliveryType: ru.Type,
                                consumer: Class[_ <: Factory[_]],
                                producer: Class[_ <: Factory[_]]): Option[Deliverable[_]] = {

    val availableDeliverable = findDeliverableByType(deliveryType)

    availableDeliverable.length match {
      case 0 =>
        log.warn("No deliverable available")
        None
      case 1 =>
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
            matchedByProducer.filter(_.consumer.nonEmpty).find(_.consumer.contains(consumer))

        }
    }
  }

  /**
    * Get all the [[Deliverable]] of the given type
    *
    * @param deliveryType type of data
    * @return
    */
  private[workflow] def findDeliverableByType(deliveryType: ru.Type): Array[Deliverable[_]] = deliveries.filter(_ == deliveryType).toArray

  /**
    * Collect a [[Deliverable]] from a [[Factory]]
    *
    * @param factory a factory object
    * @return
    */
  private[workflow] def collectDeliverable(factory: Factory[_]): this.type = setDelivery(factory.getDelivery)

  private[workflow] def dispatch(factory: Factory[_]): this.type = {
    val setters = FactoryDeliveryMetadata
      .builder()
      .setFactory(factory)
      .getOrCreate()

    dispatch(factory, setters)
  }

  /**
    * Dispatch the right deliverable object to the corresponding methods
    * (denoted by the @Delivery annotation) of a factory
    *
    * @param factory target factory
    * @return
    */
  private[workflow] def dispatch(factory: Factory[_], setters: Iterable[FactoryDeliveryMetadata]): this.type = {
    setters
      .foreach({
        setterMethod =>
          // Loop through the type of all arguments of a method and get the Deliverable that correspond to the type
          val args = setterMethod.argTypes
            .map {
              argType =>
                log.debug(s"Dispatch $argType by calling ${factory.getClass.getCanonicalName}.${setterMethod.name}")

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
      })
    this
  }

}
