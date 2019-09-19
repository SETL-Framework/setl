package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey}
import com.jcdecaux.datacorp.spark.exception.InvalidSchemaException
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}

import scala.reflect.runtime.{universe => ru}

/**
  * SchemaConverter will rename the column of a dataset/dataframe according to the given case class T.
  *
  * {{{
  *   import com.jcdecaux.datacorp.spark.annotations.colName
  *   case class MyObject(@colName("col1") column1: String, column2: String)
  *
  *   val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
  *   // +-------+-------+
  *   // |column1|column2|
  *   // +-------+-------+
  *   // |      a|      A|
  *   // |      b|      B|
  *   // +-------+-------+
  *
  *   val df = SchemaConverter.toDF(ds)
  *   // +----+-------+
  *   // |col1|column2|
  *   // +----+-------+
  *   // |   a|      A|
  *   // |   b|      B|
  *   // +----+-------+
  *
  *   val ds2 = SchemaConverter.fromDF[MyObject](df)
  *   // +-------+-------+
  *   // |column1|column2|
  *   // +-------+-------+
  *   // |      a|      A|
  *   // |      b|      B|
  *   // +-------+-------+
  *
  * }}}
  */
object SchemaConverter {

  private[this] val compoundKeySuffix: String = "_key"
  private[this] val compoundKeyPrefix: String = "_"
  private[this] val compoundKeySeparator: String = "-"

  private[this] val compoundKeyName: String => String =
    (compoundKeyId: String) => s"$compoundKeyPrefix$compoundKeyId$compoundKeySuffix"

  private[this] val compoundKeyColumn: Seq[Column] => Column =
    (columns: Seq[Column]) => functions.concat_ws(compoundKeySeparator, columns: _*)


  /**
    * Convert a DataFrame to Dataset according to the annotations
    *
    * @param dataFrame input df
    * @tparam T type of dataset
    * @return
    */
  @throws[InvalidSchemaException]
  def fromDF[T: ru.TypeTag](dataFrame: DataFrame): Dataset[T] = {
    val encoder = ExpressionEncoder[T]
    val structType = StructAnalyser.analyseSchema[T]

    val dfColumns = dataFrame.columns
    val columnsToAdd = structType
      .filter {
        field =>
          val dfContainsFieldName = dfColumns.contains(field.name)
          val dfContainsFieldAlias = if (field.metadata.contains(ColumnName.toString())) {
            dfColumns.contains(field.metadata.getStringArray(ColumnName.toString()).head)
          } else {
            false
          }
          (!dfContainsFieldName) && (!dfContainsFieldAlias)
      }

    // If there is any non-nullable missing column, throw an InvalidSchemaException
    if (!columnsToAdd.forall(_.nullable)) {
      throw new InvalidSchemaException(
        s"Find missing non-nullable column(s) [${columnsToAdd.filter(!_.nullable).map(_.name).mkString(",")}]")
    }

    val df = dataFrame
      .transform(dropCompoundKeyColumns(structType))
      .transform(replaceDFColNameByFieldName(structType))

    // Add null column for each element of columnsToAdd into df
    columnsToAdd
      .foldLeft(df)((df, field) => df.withColumn(field.name, functions.lit(null).cast(field.dataType)))
      .select(encoder.schema.map(x => functions.col(x.name)): _*) // re-order columns
      .as[T](encoder)
  }

  /**
    * Convert a dataset to a DataFrame according to annotations
    *
    * @param dataset input dataset
    * @tparam T type of dataset
    * @return
    */
  def toDF[T: ru.TypeTag](dataset: Dataset[T]): DataFrame = {
    val structType = StructAnalyser.analyseSchema[T]

    dataset
      .toDF()
      .transform(addCompoundKeyColumns(structType))
      .transform(replaceFieldNameByColumnName(structType))
  }

  /**
    * {{{
    *    import com.jcdecaux.datacorp.spark.annotations.ColumnName
    *
    *    case class MyObject(@ColumnName("col1") column1: String, column2: String)
    *
    *    convert
    *    +----+-------+
    *    |col1|column2|
    *    +----+-------+
    *    |   a|      A|
    *    |   b|      B|
    *    +----+-------+
    *
    *    to
    *    +-------+-------+
    *    |column1|column2|
    *    +-------+-------+
    *    |      a|      A|
    *    |      b|      B|
    *    +-------+-------+
    *
    * }}}
    *
    * @param structType
    * @param dataFrame
    * @return
    */
  def replaceDFColNameByFieldName(structType: StructType)(dataFrame: DataFrame): DataFrame = {
    val changes = structType
      .filter(_.metadata.contains(ColumnName.toString()))
      .map(x => x.metadata.getStringArray(ColumnName.toString())(0) -> x.name)
      .toMap

    val newColumns = dataFrame.columns.map(columnName => changes.getOrElse(columnName, columnName))

    dataFrame.toDF(newColumns: _*)
  }

  /**
    * {{{
    *    import com.jcdecaux.datacorp.spark.annotations.ColumnName
    *
    *    case class MyObject(@ColumnName("col1") column1: String, column2: String)
    *
    *    convert
    *    +----+-------+
    *    |col1|column2|
    *    +----+-------+
    *    |   a|      A|
    *    |   b|      B|
    *    +----+-------+
    *
    *    to
    *    +-------+-------+
    *    |column1|column2|
    *    +-------+-------+
    *    |      a|      A|
    *    |      b|      B|
    *    +-------+-------+
    * }}}
    *
    * @param structType
    * @param dataFrame
    * @return
    */
  def replaceFieldNameByColumnName(structType: StructType)(dataFrame: DataFrame): DataFrame = {
    val changes = structType
      .filter(_.metadata.contains(ColumnName.toString()))
      .map(x => x.name -> x.metadata.getStringArray(ColumnName.toString())(0))
      .toMap

    val newColumns = dataFrame.columns.map(columnName => changes.getOrElse(columnName, columnName))

    dataFrame.toDF(newColumns: _*)
  }

  /**
    * Drop any column that starts with "<i>_</i>" and ends with "<i>_key</i>"
    *
    * @return
    */
  def dropCompoundKeyColumns(structType: StructType)(dataFrame: DataFrame): DataFrame = {

    val columnsToDrop = structType
      .filter(_.metadata.contains(CompoundKey.toString()))
      .map(_.metadata.getStringArray(CompoundKey.toString())(0))
      .toSet

    columnsToDrop
      .foldLeft(dataFrame)((df, col) => df.drop(compoundKeyName(col)))

    // TODO do it safely
    //    dataFrame.drop(dataFrame.columns.filter(col => col.startsWith("_") && col.endsWith(compoundKeySuffix)): _*)
  }

  /**
    * {{{
    *   import com.jcdecaux.datacorp.spark.annotations.CombinedKey
    *   case class MyObject(@CombinedKey("primary", "2") column1: String,
    *                       @CombinedKey("primary", "1") column2: String)
    *
    *   from
    *   +-------+-------+
    *   |column1|column2|
    *   +-------+-------+
    *   |      a|      A|
    *   |      b|      B|
    *   +-------+-------+
    *
    *   create
    *   +-------+-------+------------+
    *   |column1|column2|_primary_key|
    *   +-------+-------+------------+
    *   |      a|      A|         A-a|
    *   |      b|      B|         B-b|
    *   +-------+-------+------------+
    * }}}
    *
    * @param structType
    * @param dataFrame
    * @return
    */
  private[this] def addCompoundKeyColumns(structType: StructType)(dataFrame: DataFrame): DataFrame = {
    val keyColumns = structType
      .filter(_.metadata.contains(CompoundKey.toString()))
      .groupBy(_.metadata.getStringArray(CompoundKey.toString())(0))
      .map {
        row =>
          val sortedCols = row._2
            .sortBy(_.metadata.getStringArray(CompoundKey.toString())(1).toInt)
            .map(n => functions.col(n.name))
          (row._1, sortedCols)
      }

    // For each element in keyColumns, add a new column to the input dataFrame
    keyColumns
      .foldLeft(dataFrame)(
        (df, col) => df.withColumn(compoundKeyName(col._1), compoundKeyColumn(col._2))
      )
  }

  //  private[this] def compressColumn(structType: StructType)(dataFrame: DataFrame): DataFrame = {
  //
  //  }

}
