package com.jcdecaux.datacorp.spark.internal

import scala.reflect.runtime

private[spark] case class FactoryOutput(outputType: runtime.universe.Type, consumer: List[Class[_]]) {

}
