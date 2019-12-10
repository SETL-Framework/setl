package com.jcdecaux.datacorp.spark.util

import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.storage.Condition
import org.apache.spark.sql.Dataset

object FilterImplicits extends Logging {

  implicit class DatasetFilterByCondition[T](dataset: Dataset[T]) {

    def filter(conditions: Set[Condition]): Dataset[T] = {
      dataset.filter(conditions.toSqlRequest)
    }

    def filter(condition: Condition): Dataset[T] = {
      dataset.filter(condition.toSqlRequest)
    }
  }

  implicit class ConditionsToRequest(conditions: Set[Condition]) {

    /**
     * Convert a [[Set]] of [[com.jcdecaux.datacorp.spark.storage.Condition]] objects to a spark SQL query string
     *
     * @throws IllegalArgumentException if a datetime/date filter doesn't have a value with correct format,
     *                                  an illegal argument exception will be thrown
     * @return String
     */
    @throws[IllegalArgumentException]
    def toSqlRequest: String = {
      val query = conditions
        .filter(row => row.value.isDefined)
        .map(_.toSqlRequest)
        .filter(_ != null)
        .mkString(" AND ")
      query
    }
  }

}
