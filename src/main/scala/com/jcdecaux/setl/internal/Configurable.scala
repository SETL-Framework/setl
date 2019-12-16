package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.InterfaceStability

@InterfaceStability.Evolving
trait Configurable {

  def set(key: String, value: String): this.type

  def get(key: String): Option[String]

}
