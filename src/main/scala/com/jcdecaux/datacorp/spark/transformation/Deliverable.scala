package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{HasType, Identifiable}
import com.jcdecaux.datacorp.spark.workflow.External

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
  * A deliverable is a container of an object that will be transferred by a [[com.jcdecaux.datacorp.spark.workflow.DeliverableDispatcher]].
  *
  * @param payload data that will be transferred
  * @tparam T type of the payload
  */
@InterfaceStability.Unstable
class Deliverable[T: ru.TypeTag](val payload: T) extends Identifiable with HasType {

  private var empty: Boolean = false
  private[this] var _producer: Class[_ <: Factory[_]] = classOf[External]
  private[this] var _deliveryId: String = Deliverable.DEFAULT_ID
  private[spark] def isEmpty: Boolean = empty

  def producer: Class[_ <: Factory[_]] = _producer

  def deliveryId: String = _deliveryId

  def setDeliveryId(id: String): this.type = {
    _deliveryId = id
    this
  }

  /**
    * Class of the consumer of this deliverable. When DispatchManager finds multiple dileverable with the same
    * type, it will select the correct deliverable by looking at the consumer
    */
  val consumer: ArrayBuffer[Class[_]] = ArrayBuffer()

  override val runtimeType: ru.Type = ru.typeTag[T].tpe

  def payloadType: ru.Type = runtimeType

  def get: T = payload

  /**
    * Compare the type of two deliverable
    *
    * @param deliverable a deliverable object
    * @return
    */
  def hasSamePayloadType(deliverable: Deliverable[_]): Boolean = this.payloadType == deliverable.payloadType

  def hasSamePayloadType(deliverable: ru.Type): Boolean = this.payloadType.equals(deliverable)

  def hasSamePayloadType(deliverableType: String): Boolean = this.payloadType.toString.equals(deliverableType)

  def hasSameContent(deliverable: Deliverable[_]): Boolean = {
    this.hasSamePayloadType(deliverable) &&
      this.consumer.intersect(deliverable.consumer).nonEmpty &&
      this.producer == deliverable.producer
  }

  def classInfo: Class[_] = payload.getClass

  /**
    * Set producer of this deliverable
    *
    * @param t class of producer
    * @return
    */
  def setProducer(t: Class[_ <: Factory[_]]): this.type = {
    _producer = t
    this
  }

  /**
    * Set consumer of this deliverable
    *
    * @param t class of consumer
    * @return
    */
  def setConsumer(t: Class[_ <: Factory[_]]): this.type = {
    consumer.append(t)
    this
  }

  def setConsumers(consumer: Class[_ <: Factory[_]]*): this.type = {
    consumer.foreach(setConsumer)
    this
  }

  def setProducer(producer: Option[Class[_ <: Factory[_]]]): this.type = {
    producer match {
      case Some(p) => setProducer(p)
      case _ => this
    }
  }

  def describe(): Unit = {
    println(s"Deliverable: $payloadType")
    println(s" From: ${producer.toString}")
    println(s" To: ${consumer.map(_.toString).mkString(", ")}")
  }

}

object Deliverable {

  val DEFAULT_ID: String = ""

  def empty(): Deliverable[Option[Nothing]] = {
    val emptyDelivery = new Deliverable[Option[Nothing]](None)
    emptyDelivery.empty = true
    emptyDelivery
  }
}
