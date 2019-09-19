package com.jcdecaux.datacorp.spark.storage.repository

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, Compress, InterfaceStability}
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.UnknownException
import com.jcdecaux.datacorp.spark.internal.{Logging, SchemaConverter, StructAnalyser}
import com.jcdecaux.datacorp.spark.storage.Condition
import com.jcdecaux.datacorp.spark.storage.connector.{Connector, DBConnector, FileConnector}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.reflect.runtime.universe.TypeTag

@InterfaceStability.Evolving
class SparkRepository[DataType: TypeTag] extends Repository[DataType] with Logging {

  private[this] var connector: Connector = _
  private[this] implicit val dataEncoder: Encoder[DataType] = ExpressionEncoder[DataType]
  private[this] val schema: StructType = StructAnalyser.analyseSchema[DataType]

  def getStorage: Storage = connector.storage

  /**
    * Set the connector of this spark repository
    *
    * @param connector [[com.jcdecaux.datacorp.spark.storage.connector.Connector]] an user defined connector
    * @return
    */
  def setConnector(connector: Connector): this.type = {
    this.connector = connector
    this
  }

  def getConnector: Connector = this.connector

  def partitionBy(columns: String*): this.type = {
    connector match {
      case c: FileConnector => c.partitionBy(columns: _*)
      case _ =>
    }
    this
  }

  private[repository] def findDataFrameBy(conditions: Set[Condition]): DataFrame = {
    import com.jcdecaux.datacorp.spark.util.FilterImplicits._

    if (conditions.nonEmpty && !conditions.toSqlRequest.isEmpty) {
      connector.read().filter(conditions.toSqlRequest)
    } else {
      connector.read()
    }
  }

  /**
    * Find data by giving a set of conditions
    *
    * @param conditions Set of [[com.jcdecaux.datacorp.spark.storage.Condition]]
    * @return
    */
  override def findBy(conditions: Set[Condition]): Dataset[DataType] = {
    val data = findDataFrameBy(SparkRepository.handleConditions(conditions, schema))
    SchemaConverter.fromDF[DataType](data)
  }

  /**
    * Retrieve all data
    */
  override def findAll(): Dataset[DataType] = {
    SchemaConverter.fromDF[DataType](connector.read())
  }

  /**
    * Save a [[Dataset]] into a data persistence store
    *
    * @param data data to be saved
    */
  override def save(data: Dataset[DataType], suffix: Option[String] = None): SparkRepository.this.type = {
    configureConnector(data.toDF(), suffix)
    connector.write(SchemaConverter.toDF[DataType](data))
    this
  }

  private[repository] def configureConnector(df: DataFrame, suffix: Option[String]): Unit = {
    connector match {
      case db: DBConnector => db.
        create(df)
      case file: FileConnector =>
        file.setSuffix(suffix)
      case _: Connector =>
      case _ =>
        throw new UnknownException.Storage(s"Unknown connector ${connector.getClass.toString}")
    }
  }
}

object SparkRepository {

  /**
    * Change the column name according to DataType schema's annotation (@ColumnName)
    *
    * @param conditions conditions
    * @return
    */
  private[repository] def handleConditions(conditions: Set[Condition], schema: StructType): Set[Condition] = {

    val columnWithAlias = schema.filter(_.metadata.contains(ColumnName.toString()))
    val binaryColumns = schema.filter(_.metadata.contains(classOf[Compress].getCanonicalName))

    conditions
      .map {
        cond =>

          if (binaryColumns.map(_.name).contains(cond.key)) {
            // TODO the following code doesn't handle the alais
            throw new IllegalArgumentException(s"Binary column ${cond.key} couldn't be filtered")
          }

          // if the current query column has an alias, we recreate a new condition and replace
          // the current key by the column name alias
          columnWithAlias.find(_.name == cond.key) match {
            case Some(a) =>
              cond.copy(key = a.metadata.getStringArray(ColumnName.toString()).head)
            case _ => cond
          }
      }
  }

}
