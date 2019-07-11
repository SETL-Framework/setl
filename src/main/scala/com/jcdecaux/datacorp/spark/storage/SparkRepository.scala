package com.jcdecaux.datacorp.spark.storage

import java.io.IOException

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.exception.UnknownException
import com.jcdecaux.datacorp.spark.storage.cassandra.CassandraConnector
import com.jcdecaux.datacorp.spark.storage.csv.CSVConnector
import com.jcdecaux.datacorp.spark.storage.parquet.ParquetConnector
import com.jcdecaux.datacorp.spark.util.SqlExpressionUtils
import org.apache.spark.sql._

/**
  * StorageSparkRepository
  *
  * @tparam T
  */
trait SparkRepository[T] extends Repository[T] with CassandraConnector with CSVConnector with ParquetConnector {

  val storage: Storage

  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findAll("")
  }

  /**
    *
    * @param encoder
    * @return
    */
  @throws[IOException]("Cassandra table does not exist")
  @throws[AnalysisException]("Path does not exist")
  def findAll(suffix: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    read().as[T]
  }

  def findBy(filter: Filter)(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findBy(Set(filter))
  }

  /**
    *
    * @param filter
    * @param encoder
    * @return
    */
  def findBy(filter: Filter, suffix: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findBy(Set(filter))
  }

  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findBy(filters, "")
  }

    /**
    *
    * @param filters
    * @param encoder
    * @return
    */
  def findBy(filters: Set[Filter], suffix: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    val df = read()
    if (filters.nonEmpty && !SqlExpressionUtils.build(filters).isEmpty) {
      df.filter(SqlExpressionUtils.build(filters))
        .as[T]
    } else {
      df.as[T]
    }
  }

  /**
    *
    * @param encoder
    * @return
    */
  @throws[IOException]("Cassandra table does not exist")
  @throws[AnalysisException]("Path does not exist")
  private[spark] def read(suffix: String = "")(implicit encoder: Encoder[T]): DataFrame = {
    storage match {
      case Storage.CASSANDRA =>
        this.readCassandra(suffix)
      case Storage.PARQUET =>
        this.readParquet(suffix)
      case Storage.CSV =>
        this.readCSV(suffix)
      case _ =>
        throw new UnknownException.Storage("Unsupported storage " + storage.name() + " exception !")
    }
  }

  def findByCondition(condition: Condition)(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findByCondition(Set(condition), "")
  }

  def findByCondition(condition: Condition, suffix: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findByCondition(Set(condition))
  }

  def findByCondition(conditions: Set[Condition])(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findByCondition(conditions, "")
  }

  def findByCondition(conditions: Set[Condition], suffix: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    import com.jcdecaux.datacorp.spark.util.FilterImplicits._

    val df = read(suffix)
    if (conditions.nonEmpty && !conditions.toSqlRequest.isEmpty) {
      df.filter(conditions.toSqlRequest)
        .as[T]
    } else {
      df.as[T]
    }
  }

  /**
    *
    * @param data
    * @param encoder
    * @return
    */
  def save(data: Dataset[T])(implicit encoder: Encoder[T]): this.type = {
    save(data, SaveMode.Overwrite)
    this
  }

  /**
    *
    * @param data
    * @param saveMode Only usable for file storage (Parquet and CSV)
    * @param encoder
    * @return
    */
  def save(data: Dataset[T], saveMode: SaveMode = SaveMode.Overwrite, suffix: String = "")(implicit encoder: Encoder[T]): this.type = {
    storage match {
      case Storage.CASSANDRA =>
        this.createCassandra(data.toDF())
        this.writeCassandra(data.toDF())
      case Storage.PARQUET =>
        this.writeParquet(data.toDF(), saveMode, suffix)
      case Storage.CSV =>
        this.writeCSV(data.toDF(), saveMode, suffix)
      case _ =>
        throw new UnknownException.Storage("Unsupported storage " + storage.name() + " exception !")
    }
    this
  }
}
