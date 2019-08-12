package com.jcdecaux.datacorp.spark.internal

import scala.reflect.runtime

trait HasType {

  val runtimeType: runtime.universe.Type

}
