package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.{Deliverable, Logging}

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
trait Factory[A] extends Logging {
  /**
    * Read data
    *
    * @return
    */
  def read(): this.type

  /**
    * Process data
    *
    * @return
    */
  def process(): this.type

  /**
    * Write data
    *
    * @return
    */
  def write(): this.type

  /**
    * Get the processed data
    *
    * @return
    */
  def get(): A

  def deliver()(implicit tag: ru.TypeTag[A]): Deliverable[A] = {
    new Deliverable[A](this.get()).setProducer(tag.tpe)
  }

  def describe()(implicit tag: ru.TypeTag[A]): Unit = {
    log.info(tag.tpe)
  }
}
