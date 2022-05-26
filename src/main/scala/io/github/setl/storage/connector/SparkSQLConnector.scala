package io.github.setl.storage.connector

import io.github.setl.config.Conf
import io.github.setl.enums.Storage
import org.apache.spark.sql.DataFrame

class SparkSQLConnector(val query: String) extends Connector {
  override val storage: Storage = Storage.SPARK_SQL

  def this(conf: Conf) = this(conf.get("query", ""))

  /**
   * Read data from the data source
   *
   * @return a [[DataFrame]]
   */
  @throws[org.apache.spark.sql.AnalysisException](s"$query is invalid")
  override def read(): DataFrame = spark.sql(query)

  /**
   * Write a [[DataFrame]] into the data storage
   *
   * @param t      a [[DataFrame]] to be saved
   * @param suffix for data connectors that support suffix (e.g. [[FileConnector]],
   *               add the given suffix to the save path
   */
  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    if (suffix.isDefined) logWarning("suffix is not supported in SparkSQLConnector")
    write(t)
  }

  /**
   * Write a [[DataFrame]] into the data storage
   *
   * @param t a [[DataFrame]] to be saved
   */
  override def write(t: DataFrame): Unit = {
    logWarning("write is not supported in SparkSQLConnector")
  }
}
