package com.jcdecaux.setl.internal

trait CanDelete {

  /**
   * Delete rows according to the query
   *
   * @param query a query string
   */
  def delete(query: String): Unit

}
