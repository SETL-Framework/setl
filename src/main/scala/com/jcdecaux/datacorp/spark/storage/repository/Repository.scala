package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.storage.Condition
import org.apache.spark.sql.{Dataset, Encoder}

/**
  * The goal of Repository is to significantly reduce the amount of boilerplate code required to
  * implement data access layers for various persistence stores.
  *
  * @tparam DataType
  */
@InterfaceStability.Evolving
trait Repository[DataType] {

  /**
    * Find data by giving a set of conditions
    *
    * @param conditions Set of [[Condition]]
    * @param encoder    : implicit encoder of Spark
    * @return
    */
  def findBy(conditions: Set[Condition])(implicit encoder: Encoder[DataType]): Dataset[DataType]

  /**
    * Find data by giving a single condition
    *
    * @param condition a [[Condition]]
    * @param encoder   : implicit encoder of Spark
    * @return
    */
  def findBy(condition: Condition)(implicit encoder: Encoder[DataType]): Dataset[DataType]

  /**
    * Retrieve all data
    *
    * @param encoder : implicit encoder of Spark
    * @return
    */
  def findAll()(implicit encoder: Encoder[DataType]): Dataset[DataType]

  /**
    * Save a [[Dataset]] into a data persistence store
    *
    * @param data    data to be saved
    * @param encoder : implicit encoder of Spark
    * @return
    */
  def save(data: Dataset[DataType])(implicit encoder: Encoder[DataType]): this.type
}
