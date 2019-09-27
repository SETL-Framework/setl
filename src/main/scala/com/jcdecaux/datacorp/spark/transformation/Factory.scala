package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{HasDescription, Identifiable, Logging}

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
abstract class Factory[A: ru.TypeTag] extends Logging with Identifiable with HasDescription {

  var consumers: List[Class[_ <: Factory[_]]] = List()

  /**
    * Read data
    */
  def read(): this.type

  /**
    * Process data
    */
  def process(): this.type

  /**
    * Write data
    */
  def write(): this.type

  /**
    * Get the processed data
    */
  def get(): A

  /**
    * Create a new Deliverable object
    */
  def getDelivery: Deliverable[A] = new Deliverable[A](this.get()).setProducer(this.getClass).setConsumers(consumers: _*)

  /**
    * Get the type of deliverable payload
    *
    * @return
    */
  def deliveryType(): ru.Type = ru.typeTag[A].tpe

  /**
    * Describe the
    */
  override def describe(): this.type = {
    log.info(s"$getPrettyName will produce a ${getPrettyName(deliveryType())}")
    this
  }

}
