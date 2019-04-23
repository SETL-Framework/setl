package com.jcdecaux.datacorp.spark.factory

import org.apache.spark.sql.Dataset

/**
  * Factory could be used to manipulate data. A Factory is able to read data from a data source, process/transform it
  * and write it back to the storage.</br>
  *
  * @tparam A the type of object that the factory is supposed to produce
  */
trait Factory[+A] {

  private var inputs: Map[String, Dataset[_]] = Map()

  /**
    *
    * @return
    */
  def setInputs(inputs: Map[String, Dataset[_]]): Factory[A] = {
    this.inputs = inputs
    this
  }

  def getInputs: Map[String, Dataset[_]] = {
    inputs
  }

  def getInput(classOf: Class[_]): Option[Dataset[_]] = {
    inputs.get(classOf.getCanonicalName)
  }

  /**
    *
    * @return
    */
  def read(): Factory[A]

  /**
    *
    * @return
    */
  def process(): Factory[A]

  /**
    *
    * @return
    */
  def write(): Factory[A]

  def get(): A
}
