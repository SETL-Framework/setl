package com.jcdecaux.setl

import java.sql.{Date, Timestamp}

import com.jcdecaux.setl.config.Conf
import com.jcdecaux.setl.internal.CanDrop
import com.jcdecaux.setl.storage.connector.ConnectorInterface
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame


case class TestObject(partition1: Int, partition2: String, clustering1: String, value: Long)

case class TestObject3(partition1: Int, partition2: String, clustering1: String, value: Long, value2: String)

case class TestObject2(col1: String, col2: Int, col3: Double, col4: Timestamp, col5: Date, col6: Long)

class CustomConnector extends ConnectorInterface with CanDrop {
  override def setConf(conf: Conf): Unit = null

  override def read(): DataFrame = {
    import spark.implicits._
    Seq(1, 2, 3).toDF("id")
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = log.debug("Write with suffix")

  override def write(t: DataFrame): Unit = log.debug("Write")

  /**
   * Drop the entire table.
   */
  override def drop(): Unit = log.debug("drop")
}