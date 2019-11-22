package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

import scala.reflect.runtime

@InterfaceStability.Evolving
trait HasType {

  val runtimeType: runtime.universe.Type

}
