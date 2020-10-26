package com.jcdecaux.setl.internal

/**
 * Connectors that inherit CanVacuum should be able to recursively delete files and directories in the table that are
 * not needed by the table for maintaining older versions up to the given retention threshold
 */
trait CanVacuum { Connector =>

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * @param retentionHours The retention threshold in hours. Files required by the table for
   *                       reading versions earlier than this will be preserved and the
   *                       rest of them will be deleted.
   */
  def vacuum(retentionHours: Double): Unit

  /**
   * Recursively delete files and directories in the table that are not needed by the table for
   * maintaining older versions up to the given retention threshold. This method will return an
   * empty DataFrame on successful completion.
   *
   * note: This will use the default retention period of 7 days.
   */
  def vacuum(): Unit

}
