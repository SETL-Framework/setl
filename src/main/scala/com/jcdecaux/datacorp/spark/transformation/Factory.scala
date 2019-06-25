package com.jcdecaux.datacorp.spark.transformation

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.internal.Deliverable

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
trait Factory[A] {
  /**
    * Read data
    *
    * @return
    */
  def read(): Factory[A]

  /**
    * Process data
    *
    * @return
    */
  def process(): Factory[A]

  /**
    * Write data
    *
    * @return
    */
  def write(): Factory[A]

  /**
    * Get the processed data
    *
    * @return
    */
  def get(): A

  def deliver()(implicit tag: ru.TypeTag[A]): Deliverable[A] = new Deliverable[A](this.get())
}
