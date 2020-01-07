package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.internal.{HasType, Identifiable}
import com.jcdecaux.setl.workflow.External

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
 * A deliverable is a container of an object that will be transferred by a [[com.jcdecaux.setl.workflow.DeliverableDispatcher]].
 *
 * @param payload data that will be transferred
 * @tparam T type of the payload
 */
@InterfaceStability.Evolving
class Deliverable[T: ru.TypeTag](payload: T) extends Identifiable with HasType {

  private var empty: Boolean = false
  private[this] var _producer: Class[_ <: Factory[_]] = classOf[External]
  private[this] val _consumer: ArrayBuffer[Class[_ <: Factory[_]]] = ArrayBuffer()
  private[this] var _deliveryId: String = Deliverable.DEFAULT_ID

  private[setl] def isEmpty: Boolean = empty

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
  def consumer: List[Class[_ <: Factory[_]]] = _consumer.toList

  override val runtimeType: ru.Type = ru.typeTag[T].tpe

  def payloadType: ru.Type = runtimeType

  @throws[NoSuchElementException]("Thrown when the Payload is null")
  def getPayload: T = {
    if (payload == null) {
      throw new NoSuchElementException(s"Payload of type ${this.runtimeType} is not set.")
    } else {
      payload
    }
  }

  /**
   * Compare the type of two deliverable
   *
   * @param deliverable a deliverable object
   * @return
   */
  def hasSamePayloadType(deliverable: Deliverable[_]): Boolean = this.payloadType =:= deliverable.payloadType

  def hasSamePayloadType(deliverable: ru.Type): Boolean = this.payloadType =:= deliverable

  def hasSamePayloadType(deliverableType: String): Boolean = this.payloadType.toString.equals(deliverableType)

  def hasSameContent(deliverable: Deliverable[_]): Boolean = {
    this.hasSamePayloadType(deliverable) &&
      this.consumer.intersect(deliverable.consumer).nonEmpty &&
      this.producer == deliverable.producer
  }

  def payloadClass: Class[_] = payload.getClass

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
   * Set consumer to this deliverable
   *
   * @param consumer class of consumer
   * @return
   */
  def setConsumer(consumer: Class[_ <: Factory[_]]): this.type = {
    this._consumer.append(consumer)
    this
  }

  /**
   * Set consumers to this deliverable
   *
   * @param consumer class of consumer
   * @return
   */
  def setConsumers(consumer: Seq[Class[_ <: Factory[_]]]): this.type = {
    this._consumer.appendAll(consumer)
    this
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
