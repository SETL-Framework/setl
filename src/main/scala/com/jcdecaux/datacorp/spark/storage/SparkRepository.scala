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
  * @tparam T
  */
trait SparkRepository[T] extends Repository[T] with CassandraConnector with CSVConnector with ParquetConnector {

  val storage: Storage

  /**
    *
    * @param encoder
    * @return
    */
  @throws[IOException]("Cassandra table does not exist")
  @throws[AnalysisException]("Path does not exist")
  private def read()(implicit encoder: Encoder[T]): DataFrame = {
    storage match {
      case Storage.CASSANDRA =>
        this.readCassandra()
      case Storage.PARQUET =>
        this.readParquet()
      case Storage.CSV =>
        this.readCSV()
      case _ =>
        throw new UnknownException("Unsupported storage " + storage.name() + " exception !")
    }
  }

  /**
    *
    * @param encoder
    * @return
    */
  @throws[IOException]("Cassandra table does not exist")
  @throws[AnalysisException]("Path does not exist")
  def findAll()(implicit encoder: Encoder[T]): Dataset[T] = {
    read().as[T]
  }

  /**
    *
    * @param filters
    * @param encoder
    * @return
    */
  def findBy(filters: Set[Filter])(implicit encoder: Encoder[T]): Dataset[T] = {
    val df = read()
    if(filters.nonEmpty && !SqlExpressionUtils.build(filters).isEmpty) {
      df.filter(SqlExpressionUtils.build(filters))
        .as[T]
    } else {
      df.as[T]
    }
  }

  /**
    *
    * @param filter
    * @param encoder
    * @return
    */
  def findBy(filter: Filter)(implicit encoder: Encoder[T]): Dataset[T] = {
    this.findBy(Set(filter))
  }

  /**
    *
    * @param data
    * @param saveMode Only usable for file storage (Parquet and CSV)
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
  def save(data: Dataset[T], saveMode: SaveMode)(implicit encoder: Encoder[T]): this.type = {
    storage match {
      case Storage.CASSANDRA =>
        this.createCassandra(data.toDF())
        this.writeCassandra(data.toDF())
      case Storage.PARQUET =>
        this.writeParquet(data.toDF(), saveMode)
      case Storage.CSV =>
        this.writeCSV(data.toDF(), saveMode)
      case _ =>
        throw new UnknownException("Unsupported storage " + storage.name() + " exception !")
    }
    this
  }
}