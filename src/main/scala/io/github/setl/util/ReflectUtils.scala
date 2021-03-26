package io.github.setl.util

import scala.reflect.runtime

object ReflectUtils {

  def getPrettyName(tpe: runtime.universe.Type): String = tpe.toString.split("\\[").map(getPrettyName).mkString("[")

  def getPrettyName(cls: Class[_]): String = getPrettyName(cls.getCanonicalName)

  def getPrettyName(canonicalName: String): String = canonicalName.split("\\.").last

}
