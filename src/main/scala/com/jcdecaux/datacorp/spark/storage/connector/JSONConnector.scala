package com.jcdecaux.datacorp.spark.storage.connector

import java.nio.charset.StandardCharsets

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.{Conf, ConnectorConf}
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Connector that loads JSON files and returns the results as a `DataFrame`.
 *
 * You can set the following JSON-specific options to deal with non-standard JSON files:
 * <ul>
 * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
 * <li>`prefersDecimal` (default `false`): infers all floating-point values as a decimal
 * type. If the values do not fit in decimal, then it infers them as doubles.</li>
 * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
 * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
 * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
 * </li>
 * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
 * (e.g. 00012)</li>
 * <li>`allowBackslashEscapingAnyCharacter` (default `false`): allows accepting quoting of all
 * character using backslash quoting mechanism</li>
 * <li>`allowUnquotedControlChars` (default `false`): allows JSON Strings to contain unquoted
 * control characters (ASCII characters with value less than 32, including tab and line feed
 * characters) or not.</li>
 * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
 * during parsing.
 * <ul>
 * <li>`PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a
 * field configured by `columnNameOfCorruptRecord`, and sets other fields to `null`. To
 * keep corrupt records, an user can set a string type field named
 * `columnNameOfCorruptRecord` in an user-defined schema. If a schema does not have the
 * field, it drops corrupt records during parsing. When inferring a schema, it implicitly
 * adds a `columnNameOfCorruptRecord` field in an output schema.</li>
 * <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
 * <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
 * </ul>
 * </li>
 * <li>`columnNameOfCorruptRecord` (default is the value specified in
 * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
 * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
 * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
 * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
 * date type.</li>
 * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`): sets the string that
 * indicates a timestamp format. Custom date formats follow the formats at
 * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
 * <li>`multiLine` (default `false`): parse one record, which may span multiple lines,
 * per file</li>
 * <li>`encoding` (by default it is not set): allows to forcibly set one of standard basic
 * or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. If the encoding
 * is not specified and `multiLine` is set to `true`, it will be detected automatically.</li>
 * <li>`lineSep` (default covers all `\r`, `\r\n` and `\n`): defines the line separator
 * that should be used for parsing.</li>
 * <li>`samplingRatio` (default is 1.0): defines fraction of input JSON objects used
 * for schema inferring.</li>
 * <li>`dropFieldIfAllNull` (default `false`): whether to ignore column of all null values or
 * empty array/struct during schema inference.</li>
 * </ul>
 **/
@InterfaceStability.Evolving
class JSONConnector(override val options: ConnectorConf) extends FileConnector(options) {

  def this(options: Map[String, String]) = this(ConnectorConf.fromMap(options))

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(conf: Conf) = this(conf.toMap)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, options: ConnectorConf) = this(options)

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, options: Map[String, String]) = this(spark, ConnectorConf.fromMap(options))

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, config: Config) = this(spark = spark, options = TypesafeConfigUtils.getMap(config))

  @deprecated("use the constructor with no spark session", "0.3.4")
  def this(spark: SparkSession, conf: Conf) = this(spark = spark, options = conf.toMap)

  override val storage: Storage = Storage.JSON
  options.setStorage(storage)

  private[this] val standardJSONPath: Path = if (!absolutePath.getName.endsWith(".json")) {
    absolutePath.suffix(".json")
  } else {
    absolutePath
  }

  def getStandardJSONPath: Path = this.standardJSONPath

  /**
   * Write a JSON file in the standard format.
   *
   * <p>This method will collect all the DataFrame partitions to the spark driver so it may impact the performance
   * when the amount of data to write is huge.</p>
   *
   * @param df DataFrame to be written
   * @return
   */
  def writeStandardJSON(df: DataFrame): this.type = {
    val bytes = df.toJSON
      .collect()
      .mkString("[", ",", "]")
      .getBytes(StandardCharsets.UTF_8)

    val overwrite = options.getSaveMode match {
      case SaveMode.Overwrite => true
      case _ => false
    }

    val hdfsOutputStream = this.getFileSystem.create(standardJSONPath, overwrite)

    try {
      hdfsOutputStream.write(bytes)
    } finally {
      hdfsOutputStream.close()
    }

    this
  }

  def readStandardJSON(): String = {
    val hdfsInputStream = this.getFileSystem.open(standardJSONPath)
    try {
      IOUtils.toString(hdfsInputStream, StandardCharsets.UTF_8)
    } finally {
      hdfsInputStream.close()
    }
  }

  def deleteStandardJSON(): this.type = {
    this.getFileSystem.delete(standardJSONPath, true)
    this
  }
}
