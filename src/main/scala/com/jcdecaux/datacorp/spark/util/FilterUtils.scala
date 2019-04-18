package com.jcdecaux.datacorp.spark.util

import com.jcdecaux.datacorp.spark.storage.Filter
import org.apache.log4j.Logger

object FilterUtils {

  private[this] val logger: Logger = Logger.getLogger(this.getClass)

  implicit class FiltersToRequest(filters: Set[Filter]) {

    /**
      * Convert a [[Set]] of [[com.jcdecaux.datacorp.spark.storage.Filter]] objects to a spark SQL query string
      *
      * @throws IllegalArgumentException if a datetime/date filter doesn't have a value with correct format,
      *                                  an illegal argument exception will be thrown
      * @return String
      */
    @throws[IllegalArgumentException]
    def toSqlRequest: String = {
      val query = filters
        .filter(row => row.value.isDefined)
        .map(_.toSqlRequest)
        .mkString(" AND ")

      logger.debug(s"Query: $query")
      query
    }
  }

  implicit class FilterToRequest(filter: Filter) {

    /**
      * Convert a [[com.jcdecaux.datacorp.spark.storage.Filter]] object to a spark SQL query string
      *
      * @throws IllegalArgumentException if a datetime/date filter doesn't have a value with correct format,
      *                                  an illegal argument exception will be thrown
      * @return String
      */
    @throws[IllegalArgumentException]
    def toSqlRequest: String = {
      val query: String = if (filter.value.isDefined) {
        filter.nature match {
          case "datetime" =>
            val t = DateUtils.reformatDateTimeString(filter.value.get, withTime = true, end = if (filter.operator.contains(">")) false else true)
            s"${filter.column} ${filter.operator} cast('$t' as timestamp)"
          case "date" =>
            val t = DateUtils.reformatDateTimeString(filter.value.get, withTime = false, end = if (filter.operator.contains(">")) false else true)
            s"${filter.column} ${filter.operator} cast('$t' as date)"
          case "string" =>
            s"${filter.column} ${filter.operator} '${filter.value.get}'"
          case _ =>
            s"${filter.column} ${filter.operator} ${filter.value.get}"
        }
      } else {
        ""
      }

      logger.debug(s"Query: $query")
      query
    }
  }

}
