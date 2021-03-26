package io.github.setl.storage.connector

import io.github.setl.annotation.InterfaceStability
import io.github.setl.config.{Conf, DynamoDBConnectorConf}
import io.github.setl.enums.Storage
import io.github.setl.internal.HasReaderWriter
import io.github.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._


/**
 * DynamoDB connector.
 *
 * {{{
 *   # Configuration
 *   dynamodb {
 *     region = ""
 *     table = ""
 *     saveMode = ""
 *   }
 * }}}
 */
@InterfaceStability.Evolving
class DynamoDBConnector(val conf: DynamoDBConnectorConf) extends DBConnector with HasReaderWriter {

  def this(conf: Map[String, String]) = this(new DynamoDBConnectorConf().set(conf))

  def this(region: String,
           table: String,
           saveMode: SaveMode,
           throughput: String) = {
    this(
      new DynamoDBConnectorConf().set(
        Map(
          DynamoDBConnectorConf.REGION -> region,
          DynamoDBConnectorConf.TABLE -> table,
          DynamoDBConnectorConf.Writer.UPDATE -> "true",
          DynamoDBConnectorConf.THROUGHPUT -> throughput
        )
      )
    )
  }

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  private[this] val sourceName: String = "com.audienceproject.spark.dynamodb.datasource"

  require(conf.getTable.nonEmpty, "DynamoDB table is not defined")
  require(conf.getRegion.nonEmpty, "DynamoDB region is not defined")

  override val reader: DataFrameReader = {
    logDebug(s"DynamoDB connector read throughput ${conf.get(DynamoDBConnectorConf.THROUGHPUT)}")
    spark.read
      .options(conf.getReaderConf)
      .format(sourceName)
  }

  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    df.write
      .options(conf.getWriterConf)
      .mode(conf.getSaveMode)
      .format(sourceName)
  }

  override val storage: Storage = Storage.DYNAMODB

  private[this] def writeDynamoDB(df: DataFrame, tableName: String): Unit = {
    this.setJobDescription(s"Write data to table $tableName}")
    conf.getWriterConf.foreach(log.debug)

    import com.audienceproject.spark.dynamodb.implicits._
    df.write
      .options(conf.getWriterConf)
      .mode(conf.getSaveMode)
      .dynamodb(tableName)
  }

  override def read(): DataFrame = {
    this.setJobDescription(s"Write data to table ${conf.getTable.getOrElse("unknown")}")
    logDebug(s"Reading DynamoDB table ${conf.getTable.get} in ${conf.getRegion.get}")
    conf.getReaderConf.foreach(log.debug)
    reader.option("tableName", conf.getTable.get).load()
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    logWarning("Suffix will be ignored in DynamoDBConnector")
    write(t)
  }

  override def create(t: DataFrame, suffix: Option[String]): Unit = {
    logWarning("Create is not supported in DynamoDBConnector")
  }

  override def delete(query: String): Unit = {
    logWarning("Delete is not supported in DynamoDBConnector")
  }

  override def create(t: DataFrame): Unit = {
    logWarning("Create is not supported in DynamoDBConnector")
  }

  override def write(t: DataFrame): Unit = {
    writeDynamoDB(t, conf.getTable.get)
  }

  override def drop(): Unit = {
    logWarning("Drop is not supported in DynamoDBConnector")
  }

}
