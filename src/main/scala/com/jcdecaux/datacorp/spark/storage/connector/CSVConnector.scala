package com.jcdecaux.datacorp.spark.storage.connector

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.Conf
import com.jcdecaux.datacorp.spark.enums.Storage
import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql._

/**
  * Connector that loads CSV files and returns the result as a `DataFrame`.
  *
  * You can set the following CSV-specific options to deal with CSV files:
  * <ul>
  * <li>`sep` (default `,`): sets a single character as a separator for each
  * field and value.</li>
  * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
  * type.</li>
  * <li>`quote` (default `"`): sets a single character used for escaping quoted values where
  * the separator can be part of the value. If you would like to turn off quotations, you need to
  * set not `null` but an empty string. This behaviour is different from
  * `com.databricks.spark.csv`.</li>
  * <li>`escape` (default `\`): sets a single character used for escaping quotes inside
  * an already quoted value.</li>
  * <li>`charToEscapeQuoteEscaping` (default `escape` or `\0`): sets a single character used for
  * escaping the escape for the quote character. The default value is escape character when escape
  * and quote characters are different, `\0` otherwise.</li>
  * <li>`comment` (default empty string): sets a single character used for skipping lines
  * beginning with this character. By default, it is disabled.</li>
  * <li>`header` (default `false`): uses the first line as names of columns.</li>
  * <li>`enforceSchema` (default `true`): If it is set to `true`, the specified or inferred schema
  * will be forcibly applied to datasource files, and headers in CSV files will be ignored.
  * If the option is set to `false`, the schema will be validated against all headers in CSV files
  * in the case when the `header` option is set to `true`. Field names in the schema
  * and column names in CSV headers are checked by their positions taking into account
  * `spark.sql.caseSensitive`. Though the default value is true, it is recommended to disable
  * the `enforceSchema` option to avoid incorrect results.</li>
  * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
  * requires one extra pass over the data.</li>
  * <li>`samplingRatio` (default is 1.0): defines fraction of rows used for schema inferring.</li>
  * <li>`ignoreLeadingWhiteSpace` (default `false`): a flag indicating whether or not leading
  * whitespaces from values being read should be skipped.</li>
  * <li>`ignoreTrailingWhiteSpace` (default `false`): a flag indicating whether or not trailing
  * whitespaces from values being read should be skipped.</li>
  * <li>`nullValue` (default empty string): sets the string representation of a null value. Since
  * 2.0.1, this applies to all supported types including the string type.</li>
  * <li>`emptyValue` (default empty string): sets the string representation of an empty value.</li>
  * <li>`nanValue` (default `NaN`): sets the string representation of a non-number" value.</li>
  * <li>`positiveInf` (default `Inf`): sets the string representation of a positive infinity
  * value.</li>
  * <li>`negativeInf` (default `-Inf`): sets the string representation of a negative infinity
  * value.</li>
  * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
  * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
  * date type.</li>
  * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`): sets the string that
  * indicates a timestamp format. Custom date formats follow the formats at
  * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
  * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
  * a record can have.</li>
  * <li>`maxCharsPerColumn` (default `-1`): defines the maximum number of characters allowed
  * for any given value being read. By default, it is -1 meaning unlimited length</li>
  * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
  * during parsing. It supports the following case-insensitive modes.
  * <ul>
  * <li>`PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a
  * field configured by `columnNameOfCorruptRecord`, and sets other fields to `null`. To keep
  * corrupt records, an user can set a string type field named `columnNameOfCorruptRecord`
  * in an user-defined schema. If a schema does not have the field, it drops corrupt records
  * during parsing. A record with less/more tokens than schema is not a corrupted record to
  *     CSV. When it meets a record having fewer tokens than the length of the schema, sets
  * `null` to extra fields. When the record has more tokens than the length of the schema,
  * it drops extra tokens.</li>
  * <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
  * <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
  * </ul>
  * </li>
  * <li>`columnNameOfCorruptRecord` (default is the value specified in
  * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
  * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
  * <li>`multiLine` (default `false`): parse one record, which may span multiple lines.</li>
  * </ul>
  *
  */
@InterfaceStability.Evolving
class CSVConnector(override val spark: SparkSession,
                   override val options: Map[String, String]) extends FileConnector(spark, options) {

  override val storage: Storage = Storage.CSV

  def this(spark: SparkSession, path: String, inferSchema: String, delimiter: String, header: String, saveMode: SaveMode) =
    this(spark, Map[String, String](
      "path" -> path,
      "inferSchema" -> inferSchema,
      "header" -> header,
      "saveMode" -> saveMode.toString
    ))

  def this(spark: SparkSession, config: Config) = this(spark = spark, options = TypesafeConfigUtils.getMap(config))

  def this(spark: SparkSession, conf: Conf) = this(spark = spark, options = conf.toMap)

  /**
    * Read a [[DataFrame]] from a csv file with the path defined during the instantiation.
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading csv file from ${absolutePath.toString}")

    val df = reader.csv(listFiles(): _*)

    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffix)) {
      df.drop(userDefinedSuffix)
    } else {
      df
    }
  }

  /**
    * Write a [[DataFrame]] into CSV file
    *
    * @param df     dataframe to be written
    * @param suffix optional, String, write the df in a sub-directory of the defined path
    */
  override def write(df: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) =>
        checkPartitionValidity(true)
        writeCSV(df, s"${this.absolutePath.toString}/$userDefinedSuffix=$s")
      case _ =>
        checkPartitionValidity(false)
        writeCSV(df, this.absolutePath.toString)
    }
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  private[this] def writeCSV(df: DataFrame, filepath: String): Unit = {
    log.debug(s"Write DataFrame to $filepath")
    initWriter(df)
    writer.csv(filepath)
  }

}
