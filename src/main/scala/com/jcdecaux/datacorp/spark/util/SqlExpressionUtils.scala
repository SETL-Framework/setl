package com.jcdecaux.datacorp.spark.util

import com.jcdecaux.datacorp.spark.storage.Filter

/**
  * SqlExpressionUtil
  */
object SqlExpressionUtils {

  import FilterUtils._

  /**
    *
    * @param filters
    * @return
    */
  def build(filters: Set[Filter]): String = filters.toSqlRequest


  /**
    *
    * @return
    */
  def request(filter: Filter): String = filter.toSqlRequest
}
