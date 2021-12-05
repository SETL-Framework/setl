package io.github.setl.internal

import io.github.setl.annotation.InterfaceStability

@InterfaceStability.Evolving
trait Configurable {

  def set(key: String, value: String): this.type

  def get(key: String): Option[String]

}
