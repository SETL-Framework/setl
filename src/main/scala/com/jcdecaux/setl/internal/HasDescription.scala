package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.util.ReflectUtils


@InterfaceStability.Evolving
trait HasDescription {

  def getPrettyName: String = ReflectUtils.getPrettyName(this.getClass)

  /** Describe the current class */
  def describe(): this.type

}
