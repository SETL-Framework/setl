package com.jcdecaux.setl.internal

import com.jcdecaux.setl.annotation.InterfaceStability

import scala.reflect.runtime

@InterfaceStability.Evolving
trait HasDescription {

  def getPrettyName(cls: Class[_]): String = getPrettyName(cls.getCanonicalName)

  def getPrettyName(canonicalName: String): String = canonicalName.split("\\.").last

  def getPrettyName(tpe: runtime.universe.Type): String = tpe.toString.split("\\[").map(getPrettyName).mkString("[")

  def getPrettyName: String = getPrettyName(this.getClass)

  /** Describe the current class */
  def describe(): this.type

}
