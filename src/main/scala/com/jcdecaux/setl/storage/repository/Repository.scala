package com.jcdecaux.setl.storage.repository

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.storage.Condition
import org.apache.spark.sql.{Column, DataFrame, Dataset}

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
  def findBy(conditions: Set[Condition]): DT

  /**
   * Find data by giving a single condition
   *
   * @param condition a [[Condition]]
   * @return
   */
  def findBy(condition: Condition): DT = this.findBy(Set(condition))

  /**
   * Find data by giving a Spark sql column
   *
   * @param column a column object (could be chained)
   * @return
   */
  def findBy(column: Column): DT = this.findBy(Condition(column))

  /**
   * Retrieve all data
   *
   * @return
   */
  def findAll(): DT

  /**
   * Save a [[Dataset]] into a data persistence store
   *
   * @param data   data to be saved
   * @param suffix an optional string to separate data
   * @return this repository instance
   */
  def save(data: DT, suffix: Option[String]): this.type


  /**
   * Update/Insert a [[Dataset]] into a data persistence store
   *
   * @param data data to be saved
   * @return this repository instance
   */
  def update(data: DT): this.type

  /**
   * Drop the entire table/file/directory
   * @return this repository instance
   */
  def drop(): this.type

  def delete(query: String): this.type

  /**
   * Create a data storage (e.g. table in a database or file/folder in a file system) with a suffix
   *
   * @param t      data frame to be written
   * @param suffix suffix to be appended at the end of the data storage name
   */
  def create(t: DataFrame, suffix: Option[String]): this.type

  /**
   * Create a data storage (e.g. table in a database or file/folder in a file system)
   *
   * @param t data frame to be written
   */
  def create(t: DataFrame): this.type

  def vacuum(retentionHours: Double): this.type

  def vacuum(): this.type

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   */
  def awaitTermination(): Unit

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   *
   * @param timeout time to wait in milliseconds
   * @return `true` if it's stopped; or throw the reported error during the execution; or `false`
   *         if the waiting time elapsed before returning from the method.
   */
  def awaitTerminationOrTimeout(timeout: Long): Boolean

  /**
   * Stops the execution of this query if it is running.
   */
  def stopStreaming(): this.type

}
