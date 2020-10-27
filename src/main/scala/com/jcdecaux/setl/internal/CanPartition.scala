package com.jcdecaux.setl.internal

import com.jcdecaux.setl.storage.connector.Connector

/**
 * Connectors that inherit CanPartition should be able to partition the output by the given columns on the file system
 */
trait CanPartition {
  self: Connector =>

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   * <ul>
   * <li>year=2016/month=01/</li>
   * <li>year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON)
   */
  def partitionBy(columns: String*): this.type

}
