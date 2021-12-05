package io.github.setl.internal

import io.github.setl.annotation.InterfaceStability
import io.github.setl.util.ReflectUtils


@InterfaceStability.Evolving
trait HasDescription {

  def getPrettyName: String = ReflectUtils.getPrettyName(this.getClass)

  /** Describe the current class */
  def describe(): this.type

}
