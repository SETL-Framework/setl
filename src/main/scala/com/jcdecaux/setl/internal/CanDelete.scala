package com.jcdecaux.setl.internal

/**
 * Connectors that inherit CanDelete should be able to delete records for a given query string
 */
trait CanDelete { Connector =>

  /**
   * Delete rows according to the query
   *
   * @param query a query string
   */
  def delete(query: String): Unit

}
