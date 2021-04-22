package io.github.setl.config

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

class JDBCConnectorConf extends ConnectorConf {

  import JDBCConnectorConf._

  def setUrl(url: String): this.type = set(URL, url)

  def getUrl: Option[String] = get(URL)

  def setDbTable(table: String): this.type = set(DB_TABLE, table)

  def getDbTable: Option[String] = get(DB_TABLE)

  def setUser(user: String): this.type = set(USER, user)

  def getUser: Option[String] = get(USER)

  def setPassword(pw: String): this.type = set(PASSWORD, pw)

  def getPassword: Option[String] = get(PASSWORD)

  def setSaveMode(saveMode: SaveMode): this.type = setSaveMode(saveMode.toString)

  def setSaveMode(saveMode: String): this.type = set(SAVEMODE, saveMode)

  def getSaveMode: Option[String] = get(SAVEMODE)

  def setNumPartitions(numPartitions: String): this.type = set(NUM_PARTITIONS, numPartitions)

  def getNumPartitions: Option[String] = get(NUM_PARTITIONS)

  def setPartitionColumn(col: String): this.type = set(PARTITION_COLUMN, col)

  def getPartitionColumn: Option[String] = get(PARTITION_COLUMN)

  def setLowerBound(lb: String): this.type = set(LOWER_BOUND, lb)

  def getLowerBound: Option[String] = get(LOWER_BOUND)

  def setUpperBound(ub: String): this.type = set(UPPER_BOUND, ub)

  def getUpperBound: Option[String] = get(UPPER_BOUND)

  def setFetchSize(size: String): this.type = set(FETCH_SIZE, size)

  def getFetchSize: Option[String] = get(FETCH_SIZE)

  def setBatchSize(size: String): this.type = set(BATCH_SIZE, size)

  def getBatchSize: Option[String] = get(BATCH_SIZE)

  def setTruncate(boo: String): this.type = set(TRUNCATE, boo)

  def getTruncate: Option[String] = get(TRUNCATE)

  def getDriver: Option[String] = get(DRIVER)

  def getFormat: Option[String] = Option(FORMAT)

  override def getReaderConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - BATCH_SIZE - TRUNCATE - SAVEMODE - FORMAT
  }

  override def getWriterConf: Map[String, String] = {
    import scala.collection.JavaConverters._
    settings.asScala.toMap - FETCH_SIZE - FORMAT - SAVEMODE - PARTITION_COLUMN - LOWER_BOUND - UPPER_BOUND
  }

  @throws[java.lang.IllegalArgumentException]
  def getJDBCOptions: JDBCOptions = {
    val parameters: Map[String, String] = Array(
      JDBCConnectorConf.USER,
      JDBCConnectorConf.PASSWORD,
      JDBCConnectorConf.URL,
      JDBCConnectorConf.DB_TABLE,
      JDBCConnectorConf.DRIVER
    ).collect {
      case key: String if this.has(key) => key -> this.get(key).get
    }.toMap

    new JDBCOptions(parameters)
  }

}

object JDBCConnectorConf {

  def fromMap(options: Map[String, String]): JDBCConnectorConf = new JDBCConnectorConf().set(options)

  val SAVEMODE: String = "saveMode"
  val FORMAT: String = "jdbc"

  val URL: String = "url"
  val DB_TABLE: String = "dbtable"
  val USER: String = "user"
  val PASSWORD: String = "password"
  val FETCH_SIZE: String = "fetchsize"
  val BATCH_SIZE: String = "batchsize"
  val TRUNCATE: String = "truncate"
  val NUM_PARTITIONS: String = "numPartitions"
  val PARTITION_COLUMN: String = "partitionColumn"
  val LOWER_BOUND: String = "lowerBound"
  val UPPER_BOUND: String = "upperBound"
  val DRIVER: String = "driver"

}
