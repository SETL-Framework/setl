package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

import scala.reflect.runtime.{universe => ru}

/**
  * A deliverable is a container of an object that will be transferred by a [[DispatchManager]].
  *
  * @param payload data that will be transferred
  * @param tag     type tag of the class T
  * @tparam T type of the payload
  */
@InterfaceStability.Unstable
class Deliverable[+T](val payload: T)(implicit tag: ru.TypeTag[T]) {

  var producer: Option[Class[_]] = None

  /**
    * Class of the consumer of this deliverable. When DispatchManager finds multiple dileverable with the same
    * type, it will select the correct deliverable by looking at the consumer
    */
  var consumer: Option[Class[_]] = None

  def tagInfo: ru.Type = tag.tpe

  def get: T = payload

  /**
    * Compare the type of two deliverable
    *
    * @param t a deliverable object
    * @return
    */
  def ==(t: Deliverable[_]): Boolean = this.tagInfo.equals(t.tagInfo)

  def classInfo: Class[_] = payload.getClass

  /**
    * Set producer of this deliverable
    *
    * @param t class of producer
    * @return
    */
  def setProducer(t: Class[_]): this.type = {
    producer = Some(t)
    this
  }

  /**
    * Set consumer of this deliverable
    *
    * @param t class of consumer
    * @return
    */
  def setConsumer(t: Class[_]): this.type = {
    consumer = Some(t)
    this
  }

}
