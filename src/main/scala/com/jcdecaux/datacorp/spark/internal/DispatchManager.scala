package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.Delivery
import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * DispatchManager will handle the data dispatch between different stages.
  *
  * It will collect a [[Deliverable]] from a [[Factory]] and send the right deliverable to another factory
  */
class DispatchManager extends Logging {

  val deliveries: ArrayBuffer[Deliverable[_]] = ArrayBuffer()

  def setDelivery(v: Deliverable[_]): this.type = {
    log.debug(s"Add new delivery of type: ${v.tagInfo}")
    deliveries.append(v)
    this
  }

  /**
    * Find the corresponding [[Deliverable]] from the pool with the given runtime Type information
    * @param t runtime type
    * @return
    */
  def getDelivery(t: ru.Type): Option[Deliverable[_]] = deliveries.find(d => d.tagInfo == t)

  def collectDeliverable(factory: Factory[_]): this.type = {
    setDelivery(factory.deliver())
  }

  /**
    * Dispatch the right deliverable object to the corresponding methods (denoted by the @Delivery annotation) of a factory
    *
    * @param factory target factory
    * @param tag     implicit runtime type tage
    * @tparam T Type of factory
    * @return
    */
  def dispatch[T <: Factory[_]](factory: T)(implicit tag: ru.TypeTag[T]): this.type = {

    getDeliveryAnnotatedMethod(factory)
      .foreach({
        methodName =>
          val args = methodName._2.map({
            argsType =>
              log.debug(s"Distribute $argsType to ${tag.tpe}.${methodName._1}")
              getDelivery(argsType) match {
                case Some(thing) => thing
                case _ => throw new NoSuchElementException(s"Can not find type $argsType from delivery manager")
              }
          })

          def method = factory.getClass.getMethod(methodName._1, args.map(_.classInfo): _*)

          method.invoke(factory, args.map(_.get.asInstanceOf[Object]): _*)
      })

    this
  }

  /**
    * Get the name and arguments type of methods having [[com.jcdecaux.datacorp.spark.annotation.Delivery]] annotation.
    *
    * @param factory factory
    * @param tag     implicit TypeTag
    * @tparam T type of factory
    * @return a Map of method name -> list of arguments type
    */
  def getDeliveryAnnotatedMethod[T](factory: T)(implicit tag: ru.TypeTag[T]): Map[String, List[ru.Type]] = {
    log.debug("Fetch methods having Delivery annotation")
    val factoryInfo = tag.tpe.typeSymbol.info
    val methodsWithDeliveryAnnotation = factoryInfo.decls
      .filter(x => x.annotations.exists(y => y.tree.tpe =:= ru.typeOf[Delivery]))

    methodsWithDeliveryAnnotation.map({
      mth =>
        log.debug(s"Find method ${tag.tpe}.${mth.name}")
        (mth.name.toString, mth.typeSignature.paramLists.head.map(_.typeSignature))
    }).toMap
  }
}
