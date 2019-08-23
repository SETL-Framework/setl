package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.storage.Condition
import org.apache.spark.sql.Dataset

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
    * @return
    */
  def findBy(conditions: Set[Condition]): Dataset[DT]

  /**
    * Find data by giving a single condition
    *
    * @param condition a [[Condition]]
    * @return
    */
  def findBy(condition: Condition): Dataset[DT] = this.findBy(Set(condition))

  /**
    * Retrieve all data
    *
    * @return
    */
  def findAll(): Dataset[DT]

  /**
    * Save a [[Dataset]] into a data persistence store
    *
    * @param data    data to be saved
    * @param suffix  an optional string to separate data
    * @return
    */
  def save(data: Dataset[DT], suffix: Option[String]): this.type
}
