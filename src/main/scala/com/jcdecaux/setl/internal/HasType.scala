package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.InterfaceStability

import scala.reflect.runtime

/**
 * HasType should be used on classed having a payload
 */
@InterfaceStability.Evolving
trait HasType {

  val runtimeType: runtime.universe.Type

}
