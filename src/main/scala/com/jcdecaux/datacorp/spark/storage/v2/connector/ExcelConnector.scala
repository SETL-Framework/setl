package com.jcdecaux.datacorp.spark.storage.v2.connector

import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.internal.Logging
import com.jcdecaux.datacorp.spark.util.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

/**
  * ExcelConnector contains functionality for transforming [[DataFrame]] into parquet files
  *
  * @param spark                   spark session
  * @param path                    file path to read/write
  * @param useHeader               Required
  * @param dataAddress             Optional, default: "A1", example: "'My Sheet'!B3:C35"
  * @param treatEmptyValuesAsNulls Optional, default: true
  * @param inferSchema             Optional, default: false
  * @param addColorColumns         Optional, default: false
  * @param timestampFormat         Optional, default: yyyy-mm-dd hh:mm:ss.000
  * @param dateFormat              Optional, default: yyyy-mm-dd
  * @param maxRowsInMemory         Optional, default None. If set, uses a streaming reader which can help with big files
  * @param excerptSize             Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
  * @param workbookPassword        Optional, default None. Requires unlimited strength JCE for older JVMs
  * @param schema                  Optional, default: Either inferred schema, or all columns are Strings
  * @param saveMode                Optional, default: overwrite.
  */
class ExcelConnector(val spark: SparkSession,
                     val path: String,
                     var useHeader: String,
                     var dataAddress: String = "A1",
                     var treatEmptyValuesAsNulls: String = "true",
                     var inferSchema: String = "false",
                     var addColorColumns: String = "false",
                     var timestampFormat: String = "yyyy-mm-dd hh:mm:ss.000",
                     var dateFormat: String = "yyyy-mm-dd",
                     var sheetName: Option[String] = Some("sheet1"),
                     var maxRowsInMemory: Option[Long] = None,
                     var excerptSize: Long = 10,
                     var workbookPassword: Option[String] = None,
                     var schema: Option[StructType] = None,
                     var saveMode: SaveMode = SaveMode.Overwrite
                    ) extends Connector with Logging {

  override val storage: Storage = Storage.EXCEL
  var reader: DataFrameReader = _
  var writer: DataFrameWriter[Row] = _

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path =
      ConfigUtils.getAs[String](config, "path").get,
    useHeader =
      ConfigUtils.getAs[String](config, "useHeader").get,
    dataAddress =
      ConfigUtils.getAs[String](config, "dataAddress").getOrElse("A1"),
    treatEmptyValuesAsNulls =
      ConfigUtils.getAs[String](config, "treatEmptyValuesAsNulls").getOrElse("true"),
    inferSchema =
      ConfigUtils.getAs[String](config, "inferSchema").getOrElse("false"),
    addColorColumns =
      ConfigUtils.getAs[String](config, "addColorColumns").getOrElse("false"),
    timestampFormat =
      ConfigUtils.getAs[String](config, "timestampFormat").getOrElse("yyyy-mm-dd hh:mm:ss.000"),
    dateFormat =
      ConfigUtils.getAs[String](config, "dateFormat").getOrElse("yyyy-mm-dd"),
    sheetName =
      ConfigUtils.getAs[String](config, "sheetName"),
    maxRowsInMemory =
      ConfigUtils.getAs[Long](config, "maxRowsInMemory"),
    excerptSize =
      ConfigUtils.getAs[Long](config, "excerptSize").getOrElse(10),
    workbookPassword =
      ConfigUtils.getAs[String](config, "workbookPassword"),
    schema =
      if (ConfigUtils.getAs[String](config, "schema").isDefined) {
        Option(StructType.fromDDL(ConfigUtils.getAs[String](config, "schema").get))
      } else {
        None
      },
    saveMode =
      if (ConfigUtils.getAs[String](config, "saveMode").isDefined) {
        SaveMode.valueOf(ConfigUtils.getAs[String](config, "saveMode").get)
      } else {
        SaveMode.Overwrite
      }
  )

  override def read(): DataFrame = {
    reader = spark
      .read
      .format("com.crealytics.spark.excel")
      .option("useHeader", useHeader)
      .option("dataAddress", dataAddress)
      .option("treatEmptyValuesAsNulls", treatEmptyValuesAsNulls)
      .option("inferSchema", inferSchema)
      .option("addColorColumns", addColorColumns)
      .option("timestampFormat", timestampFormat)
      .option("excerptSize", excerptSize)

    if (sheetName.isDefined) reader.option("sheetName", sheetName.get)
    if (maxRowsInMemory.isDefined) reader.option("maxRowsInMemory", maxRowsInMemory.get)
    if (workbookPassword.isDefined) reader.option("workbookPassword", workbookPassword.get)
    if (schema.isDefined) reader.schema(schema.get)

    reader.load(path)
  }

  override def write(df: DataFrame): Unit = {
    writer = df
      .write
      .format("com.crealytics.spark.excel")
      .option("useHeader", useHeader)
      .option("dataAddress", dataAddress)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat) // Optional, default: yy-m-d h:mm

    if (sheetName.isDefined) writer.option("sheetName", sheetName.get)

    saveMode match {
      case SaveMode.Append =>
        log.warn("It seems that the Append save mode doesn't work properly. Please make sure that your data are written correctly")
        writer.mode("append")
      case SaveMode.Overwrite => writer.mode("overwrite")
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode")
    }

    writer.save(path)
  }
}
