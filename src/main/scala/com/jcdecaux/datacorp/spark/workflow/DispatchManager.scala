package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.annotation.{Delivery, InterfaceStability}
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.transformation.{Deliverable, Factory}

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

  import DispatchManager._

  val deliveries: ArrayBuffer[Deliverable[_]] = ArrayBuffer()

  def setDelivery(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery of type: ${v.payloadType}")
    deliveries.append(v)
    this
  }

  /**
    * Find the corresponding [[Deliverable]] from the pool with the given runtime Type information
    *
    * @param deliveryType runtime type
    * @return
    */
  def getDelivery(deliveryType: ru.Type, consumer: Class[_], producer: Class[_]): Option[Deliverable[_]] = {

    val availableDeliverable = findDeliverableByType(deliveryType)

    availableDeliverable.foreach(_.describe())

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
  def findDeliverableByType(deliveryType: ru.Type): Array[Deliverable[_]] = deliveries.filter(_ == deliveryType).toArray

  /**
    * Collect a [[Deliverable]] from a [[Factory]]
    *
    * @param factory a factory object
    * @return
    */
  def collectDeliverable(factory: Factory[_]): this.type = setDelivery(factory.deliver())

  /**
    * Dispatch the right deliverable object to the corresponding methods
    * (denoted by the @Delivery annotation) of a factory
    *
    * @param factory target factory
    * @return
    */
  def dispatch(factory: Factory[_]): this.type = {

    getDeliveryAnnotatedMethod(factory)
      .foreach({
        deliveryMethod =>
          // Loop through the type of all arguments of a method and get the Deliverable that correspond to the type
          val args = deliveryMethod._2.map({
            argsType =>
              log.debug(s"Dispatch $argsType by calling ${factory.getClass.getCanonicalName}.${deliveryMethod._1}")

              getDelivery(
                deliveryType = argsType,
                consumer = factory.getClass,
                producer = deliveryMethod._3
              ) match {
                case Some(delivery) => delivery
                case _ => throw new NoSuchElementException(s"Can not find type $argsType from DispatchManager")
              }
          })

          // Invoke the method with its name and arguments
          val setterMethod = factory.getClass.getMethod(deliveryMethod._1, args.map(_.classInfo): _*)
          setterMethod.invoke(factory, args.map(_.get.asInstanceOf[Object]): _*)
      })

    this
  }

}

object DispatchManager extends Logging {
  /**
    * Get the name and arguments type of methods having [[com.jcdecaux.datacorp.spark.annotation.Delivery]] annotation.
    *
    * @param obj an object
    * @tparam T type of factory
    * @return an iterable of tuple3: method name, arg list and producer class
    */
  private[spark] def getDeliveryAnnotatedMethod[T](obj: T): Iterable[(String, List[ru.Type], Class[_])] = {
    log.debug(s"Fetch methods of ${obj.getClass} having Delivery annotation")

    // Black magic XD
    val classSymbol = ru.runtimeMirror(getClass.getClassLoader).classSymbol(obj.getClass)
    val methodsWithDeliveryAnnotation = classSymbol.info.decls.filter({
      x => x.annotations.exists(y => y.tree.tpe =:= ru.typeOf[Delivery])
    })

    if (methodsWithDeliveryAnnotation.isEmpty) log.info("No method having @Delivery annotation")

    methodsWithDeliveryAnnotation.map({
      mth =>

        if (mth.isMethod) {
          log.debug(s"Find @Delivery annotated method ${obj.getClass.getCanonicalName}.${mth.name}")

          val annotation = obj.getClass.getDeclaredMethods
            .find(_.getName == mth.name.toString).get
            .getAnnotation(classOf[Delivery])

          val producerMethod = annotation.annotationType().getDeclaredMethod("producer")

          (
            mth.name.toString,
            mth.typeSignature.paramLists.head.map(_.typeSignature),
            producerMethod.invoke(annotation).asInstanceOf[Class[_]]
          )
        } else {
          log.debug(s"Find @Delivery annotated variable ${obj.getClass.getCanonicalName}.${mth.name}")

          val annotation = obj.getClass.getDeclaredField(mth.name.toString.trim).getAnnotation(classOf[Delivery])
          val producerMethod = annotation.annotationType().getDeclaredMethod("producer")

          /*
           * If an annotated value was found, then return the default setter created by compiler, which is {valueName}_$eq.
           */
          (
            mth.name.toString.trim + "_$eq",
            List(mth.typeSignature),
            producerMethod.invoke(annotation).asInstanceOf[Class[_]]
          )
        }

    })
  }
}
