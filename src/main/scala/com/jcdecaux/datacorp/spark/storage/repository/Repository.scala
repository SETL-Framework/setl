package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.storage.Condition
import org.apache.spark.sql.{Dataset, Encoder}

/**
  * The goal of Repository is to significantly reduce the amount of boilerplate code required to
  * implement data access layers for various persistence stores.
  *
  * @tparam DT data type
  */
@InterfaceStability.Evolving
trait Repository[DT] {

  /**
    * Find data by giving a set of conditions
    *
    * @param conditions Set of [[Condition]]
    * @param encoder    implicit encoder for Spark
    * @return
    */
  def findBy(conditions: Set[Condition])(implicit encoder: Encoder[DT]): Dataset[DT]

  /**
    * Find data by giving a single condition
    *
    * @param condition a [[Condition]]
    * @param encoder   implicit encoder for Spark
    * @return
    */
  def findBy(condition: Condition)(implicit encoder: Encoder[DT]): Dataset[DT] = this.findBy(Set(condition))

  /**
    * Retrieve all data
    *
    * @param encoder implicit encoder of Spark
    * @return
    */
  def findAll()(implicit encoder: Encoder[DT]): Dataset[DT]

  /**
    * Save a [[Dataset]] into a data persistence store
    *
    * @param data    data to be saved
    * @param suffix  an optional string to separate data
    * @param encoder implicit encoder for Spark
    * @return
    */
  def save(data: Dataset[DT], suffix: Option[String])(implicit encoder: Encoder[DT]): this.type
}
