package com.jcdecaux.setl.internal

trait CanDrop {

  /**
   * Drop the entire table.
   */
  def drop(): Unit

}
