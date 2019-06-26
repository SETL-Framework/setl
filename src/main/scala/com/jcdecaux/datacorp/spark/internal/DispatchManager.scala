package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.{Delivery, InterfaceStability}
import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * DispatchManager will handle the data dispatch between different stages.
  *
  * It can:
  * <ul>
  * <li>collect a [[Deliverable]] from a [[Factory]]</li>
  * <li>find the right [[Deliverable]] from its pool and send it to a [[Factory]]</li>
  * </ul>
  */
@InterfaceStability.Evolving
private[spark] class DispatchManager extends Logging {

  import DispatchManager._

  val deliveries: ArrayBuffer[Deliverable[_]] = ArrayBuffer()

  def setDelivery(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery of type: ${v.tagInfo}")
    deliveries.append(v)
    this
  }

  /**
    * Find the corresponding [[Deliverable]] from the pool with the given runtime Type information
    *
    * @param deliveryType runtime type
    * @return
    */
  def getDelivery(deliveryType: ru.Type, consumer: Class[_]): Option[Deliverable[_]] = {

    val availableDeliverables = getDeliveries(deliveryType)

    availableDeliverables.length match {
      case 0 =>
        log.warn("No deliverable available")
        None
      case 1 =>
        log.debug("Deliverable found")
        Some(availableDeliverables.head)
      case _ =>
        log.info("Multiple Deliverable with same type were found. Try matching by consumer")
        availableDeliverables.filter(_.consumer.nonEmpty).find(_.consumer.contains(consumer))
    }
  }

  /**
    * Get all the [[Deliverable]] of the given type
    *
    * @param deliveryType type of data
    * @return
    */
  def getDeliveries(deliveryType: ru.Type): Array[Deliverable[_]] =
    deliveries.filter(d => d.tagInfo == deliveryType).toArray

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
    * @tparam T Type of factory
    * @return
    */
  def dispatch[T <: Factory[_]](factory: T): this.type = {

    getDeliveryAnnotatedMethod(factory)
      .foreach({
        methodName =>
          // Loop through the type of all arguments of a method and get the Deliverable that correspond to the type
          val args = methodName._2.map({
            argsType =>
              log.debug(s"Dispatch $argsType by calling ${factory.getClass.getCanonicalName}.${methodName._1}")

              getDelivery(argsType, factory.getClass) match {
                case Some(thing) => thing
                case _ => throw new NoSuchElementException(s"Can not find type $argsType from dispatch manager")
              }
          })

          // Invoke the method with its name and arguments
          def method = factory.getClass.getMethod(methodName._1, args.map(_.classInfo): _*)
          method.invoke(factory, args.map(_.get.asInstanceOf[Object]): _*)
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
    * @return a Map of method name -> list of arguments type
    */
  private[spark] def getDeliveryAnnotatedMethod[T](obj: T): Map[String, List[ru.Type]] = {
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
          (mth.name.toString, mth.typeSignature.paramLists.head.map(_.typeSignature))
        } else {
          log.debug(s"Find @Delivery annotated value ${obj.getClass.getCanonicalName}.${mth.name}")

          // val newMethod = classSymbol.info.decls.find(_.name.toString == mth.name.toString.trim + "_$eq").get
          // (newMethod.name.toString, newMethod.typeSignature.paramLists.head.map(_.typeSignature))

          /*
           * If an annotated value was found, then return the default setter created by compiler, which is {valueName}_$eq.
           */
          (mth.name.toString.trim + "_$eq", List(mth.typeSignature))
        }

    }).toMap
  }
}
