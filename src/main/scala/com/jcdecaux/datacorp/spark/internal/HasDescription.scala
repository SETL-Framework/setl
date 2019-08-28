package com.jcdecaux.datacorp.spark.internal

import scala.reflect.runtime

trait HasDescription {

  def getPrettyName(cls: Class[_]): String = getPrettyName(cls.getCanonicalName)

  def getPrettyName(canonicalName: String): String = canonicalName.split("\\.").last

  def getPrettyName(tpe: runtime.universe.Type): String = tpe.toString.split("\\[").map(getPrettyName).mkString("[")

  def getPrettyName: String = getPrettyName(this.getClass)

  def describe(): this.type

}
