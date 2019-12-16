package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.InterfaceStability

import scala.reflect.runtime

@InterfaceStability.Evolving
trait HasType {

  val runtimeType: runtime.universe.Type

}
