package com.jcdecaux.setl.transformation

import java.util.UUID

import com.jcdecaux.setl.annotation.{Delivery, InterfaceStability}
import org.apache.spark.sql.Dataset

import scala.language.existentials
import scala.reflect.runtime

/**
 * DeliverySetterMetadata contains information of the @Delivery annotated method, including the name,
 * argument types, the producer and optional
 *
 * @param factoryUUID UUID of factory
 * @param symbol        symbol of the method
 * @param argTypes    type of each argument
 * @param producer    the producer class for the given data
 * @param optional    true if optional
 */
@InterfaceStability.Evolving
private[setl] case class FactoryDeliveryMetadata(factoryUUID: UUID,
                                                 symbol: runtime.universe.Symbol,
                                                 argTypes: List[runtime.universe.Type],
                                                 producer: Class[_ <: Factory[_]],
                                                 optional: Boolean,
                                                 autoLoad: Boolean = false,
                                                 condition: String = "",
                                                 id: String = Deliverable.DEFAULT_ID) {

  def name: String = symbol.name.toString.trim

  /**
   * As a setter method may have multiple arguments (even though it's rare), this method will return a list of
   * [[FactoryInput]] for each of argument of setter method
   *
   * @return
   */
  def getFactoryInputs: List[FactoryInput] = argTypes.map(tp => FactoryInput(tp, producer, id))

  def isDataset: List[Boolean] = argTypes.map {
    tp => tp.toString.startsWith(runtime.universe.typeOf[Dataset[_]].toString.dropRight(2))
  }
}

private[setl] object FactoryDeliveryMetadata {

  /**
   * Build a DeliverySetterMetadata from a given class
   */
  class Builder extends com.jcdecaux.setl.Builder[Iterable[FactoryDeliveryMetadata]] {

    var cls: Class[_ <: Factory[_]] = _
    var factoryUUID: UUID = _
    var metadata: Iterable[FactoryDeliveryMetadata] = _

    def setFactory(factory: Factory[_]): this.type = {
      this.cls = factory.getClass
      this.factoryUUID = factory.getUUID
      this
    }

    /**
     * Invoke a declared method of the delivery and get the value
     *
     * @param delivery       delivery object
     * @param declaredMethod name of the method
     * @tparam T type of the returned value of the method
     * @return an object of type T
     */
    private[this] def getDeliveryParameter[T](delivery: Delivery, declaredMethod: String): T = {
      val method = delivery.annotationType().getDeclaredMethod(declaredMethod)
      method.invoke(delivery).asInstanceOf[T]
    }

    override def build(): this.type = {

      log.debug(s"Search Deliveries of ${cls.getSimpleName}")

      val classSymbol = runtime.universe.runtimeMirror(getClass.getClassLoader).classSymbol(cls)
      val symbolsWithDeliveryAnnotation = classSymbol.info.decls.filter {
        x => x.annotations.exists(y => y.tree.tpe =:= runtime.universe.typeOf[Delivery])
      }

      if (symbolsWithDeliveryAnnotation.isEmpty) log.info("No method having @Delivery annotation")

      metadata = symbolsWithDeliveryAnnotation.map {
        symbol =>
          val delivery: Delivery = if (symbol.isMethod) {
            log.debug(s"Find method `${symbol.name}` in ${cls.getSimpleName} to be delivered")
            val methods = cls
              .getDeclaredMethods
              .filter(mth => mth.getName == symbol.name.toString && mth.isAnnotationPresent(classOf[Delivery]))

            if (methods.length > 1) throw new NoSuchMethodException("Found multiple methods with save name")
            if (methods.isEmpty) throw new NoSuchElementException("Can't find any method")

            methods.head.getAnnotation(classOf[Delivery])
          } else {
            log.debug(s"Find field `${symbol.name}` in ${cls.getSimpleName} to be delivered")
            cls
              .getDeclaredField(symbol.name.toString.trim)
              .getAnnotation(classOf[Delivery])
          }

          val argTypes = if (symbol.isMethod) {
            symbol.typeSignature.paramLists.head.map(_.typeSignature)
          } else {
            List(symbol.typeSignature)
          }

          FactoryDeliveryMetadata(
            factoryUUID = factoryUUID,
            symbol = symbol,
            argTypes = argTypes,
            producer = getDeliveryParameter[Class[_ <: Factory[_]]](delivery, "producer"),
            optional = getDeliveryParameter[Boolean](delivery, "optional"),
            autoLoad = getDeliveryParameter[Boolean](delivery, "autoLoad"),
            condition = getDeliveryParameter[String](delivery, "condition"),
            id = getDeliveryParameter[String](delivery, "id")
          )
      }

      this
    }

    override def get(): Iterable[FactoryDeliveryMetadata] = metadata
  }


  /**
   * DeliverySetterMetadata Builder will create a [[com.jcdecaux.setl.transformation.FactoryDeliveryMetadata]]
   * for each setter method (user defined or auto-generated by compiler)
   */
  def builder(): Builder = new Builder()
}