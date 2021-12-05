package io.github.setl.internal

import io.github.setl.storage.connector.Connector

/**
 * Connectors that inherit CanDrop should be able to drop the entire data table
 */
trait CanDrop {
  self: Connector =>

  /**
   * Drop the entire table.
   */
  def drop(): Unit

}
