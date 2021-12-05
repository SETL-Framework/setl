package io.github.setl.config

import org.apache.spark.sql.SaveMode

class DynamoDBConnectorConf extends ConnectorConf {

  import DynamoDBConnectorConf._

  def setTable(table: String): this.type = set(TABLE, table)

  def getTable: Option[String] = get(TABLE)

  def setReadPartitions(readPartitions: String): this.type = set(Reader.READ_PARTITIONS, readPartitions)

  def getReadPartitions: Option[String] = get(Reader.READ_PARTITIONS)

  def getSaveMode: SaveMode = SaveMode.valueOf(get("saveMode", "ErrorIfExists"))

  override def getReaderConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap -
      Writer.WRITE_BATCH_SIZE -
      Writer.UPDATE -
      TABLE -
      Writer.SAVE_MODE
  }

  override def getWriterConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap -
      Reader.READ_PARTITIONS -
      Reader.MAX_PARTITION_BYTES -
      Reader.DEFAULT_PARALLELISM -
      Reader.STRONGLY_CONSISTENT_READS -
      Reader.BYTES_PER_RCU -
      Reader.FILTER_PUSHDOWN -
      TABLE -
      Writer.SAVE_MODE
  }

  def getRegion: Option[String] = get(REGION)

}

object DynamoDBConnectorConf {

  object Reader {
    val READ_PARTITIONS: String = "readPartitions"
    val MAX_PARTITION_BYTES: String = "maxPartitionBytes"
    val DEFAULT_PARALLELISM: String = "defaultParallelism"
    val STRONGLY_CONSISTENT_READS: String = "stronglyConsistentReads"
    val BYTES_PER_RCU: String = "bytesPerRCU"
    val FILTER_PUSHDOWN: String = "filterPushdown"
  }

  object Writer {
    val WRITE_BATCH_SIZE = "writeBatchSize"
    val UPDATE = "update"
    val SAVE_MODE = "saveMode"
  }

  val REGION: String = "region"
  val TABLE: String = "table"
  val THROUGHPUT: String = "throughput"
  val TARGET_CAPACITY: String = "targetCapacity"
}


