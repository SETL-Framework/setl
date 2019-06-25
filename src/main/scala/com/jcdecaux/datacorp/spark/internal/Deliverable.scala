package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

import scala.reflect.runtime.{universe => ru}

@InterfaceStability.Unstable
class Deliverable[T](val payload: T)(implicit tag: ru.TypeTag[T]) {

  def tagInfo: ru.Type = tag.tpe

  def get: T = payload

  def ==(t: Deliverable[_]): Boolean = this.tagInfo.equals(t.tagInfo)

  def classInfo = payload.getClass

}
