package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.Delivery

import scala.reflect.runtime

/**
  * DeliverySetterMetadata contains information of the @Delivery annotated method, including the name,
  * argument types, the producer and optional
  *
  * @param methodName name of the method
  * @param argTypes   type of each argument
  * @param producer   the producer class for the given data
  * @param optional   true if optional
  */
private[spark] case class DeliverySetterMetadata(methodName: String,
                                                 argTypes: List[runtime.universe.Type],
                                                 producer: Class[_],
                                                 optional: Boolean) {

  /**
    * As a setter method may have multiple arguments (even though it's rare), this method will return a list of
    * [[FactoryInput]] for each of argument of setter method
    *
    * @return
    */
  def getFactoryInputs: List[FactoryInput] = argTypes.map(tp => FactoryInput(tp, producer))

}

private[spark] object DeliverySetterMetadata {

  /**
    * Build a DeliverySetterMetadata from a given class
    */
  class Builder extends com.jcdecaux.datacorp.spark.Builder[Iterable[DeliverySetterMetadata]] {

    var cls: Class[_] = _
    var metadata: Iterable[DeliverySetterMetadata] = _

    def setClass(cls: Class[_]): this.type = {
      this.cls = cls
      this
    }

    override def build(): this.type = {

      log.debug(s"Fetch methods of ${cls.getCanonicalName} having Delivery annotation")

      // Black magic XD
      val classSymbol = runtime.universe.runtimeMirror(getClass.getClassLoader).classSymbol(cls)
      val methodsWithDeliveryAnnotation = classSymbol.info.decls.filter({
        x => x.annotations.exists(y => y.tree.tpe =:= runtime.universe.typeOf[Delivery])
      })

      if (methodsWithDeliveryAnnotation.isEmpty) log.info("No method having @Delivery annotation")

      metadata = methodsWithDeliveryAnnotation.map({
        mth =>

          if (mth.isMethod) {
            log.debug(s"Find @Delivery annotated method ${cls.getCanonicalName}.${mth.name}")

            val annotation = cls
              .getDeclaredMethods
              .find(_.getName == mth.name.toString).get
              .getAnnotation(classOf[Delivery])

            val producerMethod = annotation.annotationType().getDeclaredMethod("producer")
            val optionalMethod = annotation.annotationType().getDeclaredMethod("optional")

            DeliverySetterMetadata(
              methodName = mth.name.toString,
              argTypes = mth.typeSignature.paramLists.head.map(_.typeSignature),
              producer = producerMethod.invoke(annotation).asInstanceOf[Class[_]],
              optional = optionalMethod.invoke(annotation).asInstanceOf[Boolean]
            )
          } else {
            log.debug(s"Find @Delivery annotated variable ${cls.getCanonicalName}.${mth.name}")

            val annotation = cls
              .getDeclaredField(mth.name.toString.trim)
              .getAnnotation(classOf[Delivery])

            val producerMethod = annotation.annotationType().getDeclaredMethod("producer")
            val optionalMethod = annotation.annotationType().getDeclaredMethod("optional")

            /*
             * If an annotated value was found, then return the default setter created by compiler, which is {valueName}_$eq.
             */
            DeliverySetterMetadata(
              methodName = mth.name.toString.trim + "_$eq",
              argTypes = List(mth.typeSignature),
              producer = producerMethod.invoke(annotation).asInstanceOf[Class[_]],
              optional = optionalMethod.invoke(annotation).asInstanceOf[Boolean]
            )
          }

      })

      this
    }

    override def get(): Iterable[DeliverySetterMetadata] = metadata
  }

  def builder(): Builder = new Builder()
}