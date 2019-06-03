package com.jcdecaux.datacorp.spark.storage.v2.repository

import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.storage.Condition
import com.jcdecaux.datacorp.spark.storage.v2.connector.{Connector, EnrichConnector}
import org.apache.spark.sql.{Dataset, Encoder}

class SparkRepository[DataType] extends Repository[DataType] with Logging {

  private[this] var connector: Connector = _

  /**
    * Set the connector of this spark repository
    *
    * @param connector [[com.jcdecaux.datacorp.spark.storage.v2.connector.Connector]] an user defined connector
    * @return
    */
  def setConnector(connector: Connector): this.type = {
    this.connector = connector
    this
  }

  /**
    * Find data by giving a set of conditions
    *
    * @param conditions Set of [[com.jcdecaux.datacorp.spark.storage.Condition]]
    * @param encoder    implicit encoder of Spark
    * @return
    */
  override def findBy(conditions: Set[Condition])(implicit encoder: Encoder[DataType]): Dataset[DataType] = {
    import com.jcdecaux.datacorp.spark.util.FilterImplicits._

    val df = connector.read()
    if (conditions.nonEmpty && !conditions.toSqlRequest.isEmpty) {
      df.filter(conditions.toSqlRequest)
        .as[DataType]
    } else {
      df.as[DataType]
    }
  }

  /**
    * Find data by giving a single condition
    *
    * @param condition a [[Condition]]
    * @param encoder   : implicit encoder of Spark
    * @return
    */
  override def findBy(condition: Condition)(implicit encoder: Encoder[DataType]): Dataset[DataType] = {
    this.findBy(Set(condition))

  }

  /**
    * Retrieve all data
    *
    * @param encoder : implicit encoder of Spark
    * @return
    */
  override def findAll()(implicit encoder: Encoder[DataType]): Dataset[DataType] = {
    connector.read().as[DataType]
  }

  /**
    * Save a [[Dataset]] into a data persistence store
    *
    * @param data    data to be saved
    * @param encoder : implicit encoder of Spark
    * @return
    */
  override def save(data: Dataset[DataType])(implicit encoder: Encoder[DataType]): SparkRepository.this.type = {
    try {
      connector.asInstanceOf[EnrichConnector].create(data.toDF())
    } catch {
      case nosuchelement: NoSuchMethodException =>
        log.info("There is no create method. Save directly the dataset")
      case classCast: ClassCastException =>
        log.info("Current class has no create method. Save directly the dataset")
      case e: Throwable => throw e
    }
    connector.write(data.toDF())
    this
  }
}
