package io.github.setl.transformation

import io.github.setl.annotation.InterfaceStability
import io.github.setl.internal.{HasType, Identifiable}
import io.github.setl.workflow.External

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

/**
 * A deliverable is a container of an object that will be transferred by a [[io.github.setl.workflow.DeliverableDispatcher]].
 *
 * @param payload data that will be transferred
 * @tparam T type of the payload
 */
@InterfaceStability.Evolving
class Deliverable[T: ru.TypeTag](payload: T) extends Identifiable with HasType {

  private var empty: Boolean = false
  private[this] var _producer: Class[_ <: Factory[_]] = classOf[External]
  private[this] val _consumer: scala.collection.mutable.Set[Class[_ <: Factory[_]]] = scala.collection.mutable.Set()
  private[this] var _deliveryId: String = Deliverable.DEFAULT_ID

  private[setl] def isEmpty: Boolean = empty

  /** Return the producer of this deliverable */
  def producer: Class[_ <: Factory[_]] = _producer

  /** Return the ID of this delivery */
  def deliveryId: String = _deliveryId

  /** Set the delivery ID for this delivery */
  def setDeliveryId(id: String): this.type = {
    _deliveryId = id
    this
  }

  /**
   * Class of the consumer of this deliverable. When DispatchManager finds multiple deliverable with the same
   * type, it will select the correct deliverable by looking at the consumer
   */
  def consumer: List[Class[_ <: Factory[_]]] = _consumer.toList

  /** The Scala runtime type of this deliverable's payload */
  override val runtimeType: ru.Type = ru.typeTag[T].tpe

  /** The Scala runtime type of this deliverable's payload */
  def payloadType: ru.Type = runtimeType

  /** Get the payload */
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

  /**
   * Compare the type of two deliverable
   *
   * @param deliverable the payload type of the deliverable to be compared
   * @return
   */
  def hasSamePayloadType(deliverable: ru.Type): Boolean = this.payloadType =:= deliverable

  /**
   * Compare the type of two deliverable
   *
   * @param deliverableType the string of the payload type of the deliverable to be compared
   * @return
   */
  def hasSamePayloadType(deliverableType: String): Boolean = this.payloadType.toString.equals(deliverableType)

  /**
   * Return true if:
   * <ul>
   * <li>This deliverable has the same payload type as the other deliverable</li>
   * <li>Both of them have one or more same consumers</li>
   * <li>Both of them have the same producer</li>
   * </ul>
   */
  def sameDeliveryAs(deliverable: Deliverable[_]): Boolean = {
    val sameConsumer = if (this.consumer.nonEmpty || deliverable.consumer.nonEmpty) {
      this.consumer.intersect(deliverable.consumer).nonEmpty
    } else {
      true
    }

    this.hasSamePayloadType(deliverable) && sameConsumer && this.producer == deliverable.producer
  }

  /** Get the payload class */
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
    this._consumer.add(consumer)
    this
  }

  /**
   * Set consumers to this deliverable
   *
   * @param consumer class of consumer
   * @return
   */
  def setConsumers(consumer: Seq[Class[_ <: Factory[_]]]): this.type = {
    consumer.foreach(c => setConsumer(c))
    this
  }

  /** Describe this deliverable  */
  def describe(): Unit = {
    println(s"Deliverable: $payloadType")
    println(s" From: ${producer.toString}")
    println(s" To: ${consumer.map(_.toString).mkString(", ")}")
  }

}

object Deliverable {

  /** Default delivery id of a deliverable object */
  val DEFAULT_ID: String = ""

  /** Return an empty deliverable to be used as placeholder */
  def empty(): Deliverable[Option[Nothing]] = {
    val emptyDelivery = new Deliverable[Option[Nothing]](None)
    emptyDelivery.empty = true
    emptyDelivery
  }
}
