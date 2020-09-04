package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.internal.{HasDescription, Identifiable, Logging, Writable}
import com.jcdecaux.setl.util.ReflectUtils

import scala.reflect.runtime.{universe => ru}

/**
 * Factory could be used to manipulate data.
 *
 * A Factory is able to read data from a data source, process/transform it
 * and write it back to the storage.</br>
 *
 * @tparam A the type of object that the factory is supposed to produce
 */
@InterfaceStability.Evolving
abstract class Factory[A: ru.TypeTag] extends AbstractFactory[A]
  with Logging
  with Identifiable
  with HasDescription
  with Writable {

  private[this] val _consumers: Seq[Class[_ <: Factory[_]]] = Seq.empty
  private[this] val _deliveryId: String = Deliverable.DEFAULT_ID

  /** Return the list of consumer class */
  def consumers: Seq[Class[_ <: Factory[_]]] = this._consumers

  /** Return the delivery id of this factory */
  def deliveryId: String = this._deliveryId

  /** Read data */
  override def read(): this.type

  /** Process data */
  override def process(): this.type

  /** Write data */
  override def write(): this.type

  /** Get the processed data */
  override def get(): A

  /** Create a new Deliverable object */
  def getDelivery: Deliverable[A] = {
    new Deliverable[A](this.get())
      .setProducer(this.getClass)
      .setConsumers(consumers)
      .setDeliveryId(deliveryId)
  }

  /** Get the type of deliverable payload */
  def deliveryType(): ru.Type = ru.typeTag[A].tpe

  /** Describe the */
  override def describe(): this.type = {
    log.info(s"$getPrettyName will produce a ${ReflectUtils.getPrettyName(deliveryType())}")
    this
  }

}
