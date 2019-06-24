package com.jcdecaux.datacorp.spark.factory

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.spark.sql.Dataset

/**
  * Factory could be used to manipulate data.
  *
  * A Factory is able to read data from a data source, process/transform it
  * and write it back to the storage.</br>
  *
  * @tparam A the type of object that the factory is supposed to produce
  */
@InterfaceStability.Evolving
trait Factory[+A] {

  private var inputs: Map[String, Dataset[_]] = Map()

  /**
    * Set the input of this factory
    *
    * @return
    */
  def setInputs(inputs: Map[String, Dataset[_]]): Factory[A] = {
    this.inputs = inputs
    this
  }

  /**
    * Retrieve the input of this factory under the format of a map
    *
    * @return
    */
  def getInputs: Map[String, Dataset[_]] = {
    inputs
  }

  /**
    * Retrieve the corresponding input by specifying a class
    *
    * @param classOf
    * @return
    */
  def getInput(classOf: Class[_]): Option[Dataset[_]] = {
    inputs.get(classOf.getCanonicalName)
  }

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
}
