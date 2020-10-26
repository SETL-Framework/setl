package com.jcdecaux.setl.internal

/**
 * Connectors that inherit CanDrop should be able to drop the entire data table
 */
trait CanDrop {

  /**
   * Drop the entire table.
   */
  def drop(): Unit

}
