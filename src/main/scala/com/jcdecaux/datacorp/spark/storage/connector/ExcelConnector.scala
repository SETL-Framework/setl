package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
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
@InterfaceStability.Evolving
class ExcelConnector(val spark: SparkSession,
                     val path: String,
                     var useHeader: String,
                     var dataAddress: String = "A1",
                     var treatEmptyValuesAsNulls: String = "true",
                     var inferSchema: String = "false",
                     var addColorColumns: String = "false",
                     var timestampFormat: String = "yyyy-mm-dd hh:mm:ss.000",
                     var dateFormat: String = "yyyy-mm-dd",
                     var sheetName: Option[String] = None,
                     var maxRowsInMemory: Option[Long] = None,
                     var excerptSize: Long = 10,
                     var workbookPassword: Option[String] = None,
                     var schema: Option[StructType] = None,
                     var saveMode: SaveMode = SaveMode.Overwrite
                    ) extends Connector {

  override val storage: Storage = Storage.EXCEL
  override val reader: DataFrameReader = initReader()
  override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {

    val _writer = df
      .write
      .format("com.crealytics.spark.excel")
      .option("useHeader", useHeader)
      .option("dataAddress", dataAddress)
      .option("timestampFormat", timestampFormat)
      .option("dateFormat", dateFormat) // Optional, default: yy-m-d h:mm

    if (sheetName.isDefined) _writer.option("sheetName", sheetName.get)

    saveMode match {
      case SaveMode.Append =>
        log.warn("The Append save mode doesn't work properly in excel connecter. Please ckeck manually after the save" +
          "to ensure that your data are written correctly")
        _writer.mode("append")
      case SaveMode.Overwrite => _writer.mode("overwrite")
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode")
    }

    _writer
  }

  private[this] def initReader(): DataFrameReader = {

    val reader = spark
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

    reader
  }

  def this(spark: SparkSession, conf: Conf) = this(
    spark = spark,
    path = conf.get("path").get,
    useHeader = conf.get("useHeader").get,
    dataAddress = conf.get("dataAddress").getOrElse("A1"),
    treatEmptyValuesAsNulls = conf.get("treatEmptyValuesAsNulls").getOrElse("true"),
    inferSchema = conf.get("inferSchema").getOrElse("false"),
    addColorColumns = conf.get("addColorColumns").getOrElse("false"),
    timestampFormat = conf.get("timestampFormat").getOrElse("yyyy-mm-dd hh:mm:ss.000"),
    dateFormat = conf.get("dateFormat").getOrElse("yyyy-mm-dd"),
    sheetName = conf.get("sheetName"),
    maxRowsInMemory = conf.getAs[Long]("maxRowsInMemory"),
    excerptSize = conf.getAs[Long]("excerptSize").getOrElse(10L),
    workbookPassword = conf.get("workbookPassword"),
    schema = if (conf.getAs[String]("schema").isDefined) {
      Option(StructType.fromDDL(conf.getAs[String]("schema").get))
    } else {
      None
    },
    saveMode = SaveMode.valueOf(conf.getAs[String]("saveMode").getOrElse("Overwrite"))
  )

  def this(spark: SparkSession, config: Config) = this(
    spark = spark,
    path =
      TypesafeConfigUtils.getAs[String](config, "path").get,
    useHeader =
      TypesafeConfigUtils.getAs[String](config, "useHeader").get,
    dataAddress =
      TypesafeConfigUtils.getAs[String](config, "dataAddress").getOrElse("A1"),
    treatEmptyValuesAsNulls =
      TypesafeConfigUtils.getAs[String](config, "treatEmptyValuesAsNulls").getOrElse("true"),
    inferSchema =
      TypesafeConfigUtils.getAs[String](config, "inferSchema").getOrElse("false"),
    addColorColumns =
      TypesafeConfigUtils.getAs[String](config, "addColorColumns").getOrElse("false"),
    timestampFormat =
      TypesafeConfigUtils.getAs[String](config, "timestampFormat").getOrElse("yyyy-mm-dd hh:mm:ss.000"),
    dateFormat =
      TypesafeConfigUtils.getAs[String](config, "dateFormat").getOrElse("yyyy-mm-dd"),
    sheetName =
      TypesafeConfigUtils.getAs[String](config, "sheetName"),
    maxRowsInMemory =
      TypesafeConfigUtils.getAs[Long](config, "maxRowsInMemory"),
    excerptSize =
      TypesafeConfigUtils.getAs[Long](config, "excerptSize").getOrElse(10),
    workbookPassword =
      TypesafeConfigUtils.getAs[String](config, "workbookPassword"),
    schema =
      if (TypesafeConfigUtils.getAs[String](config, "schema").isDefined) {
        Option(StructType.fromDDL(TypesafeConfigUtils.getAs[String](config, "schema").get))
      } else {
        None
      },
    saveMode = SaveMode.valueOf(TypesafeConfigUtils.getAs[String](config, "saveMode").getOrElse("Overwrite"))
  )

  override def read(): DataFrame = {
    reader.load(path)
  }

  override def write(df: DataFrame, suffix: Option[String]): Unit = {
    if (suffix.isDefined) log.warn("Suffix is not supported in ExcelConnector")
    writer(df).save(path)
  }

  override def write(t: DataFrame): Unit = this.write(t, None)
}
