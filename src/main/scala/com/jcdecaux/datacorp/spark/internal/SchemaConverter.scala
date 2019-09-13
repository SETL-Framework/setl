package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey}
import com.jcdecaux.datacorp.spark.exception.InvalidSchemaException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, functions}

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

  private[this] val compoundKeyName: String = "_key"
  private[this] val compoundKeySeparator: String = "-"

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
    val structType = analyseSchema[T]

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
        s"Non nullable column(s) [${columnsToAdd.filter(!_.nullable).map(_.name).mkString(",")}] is not present")
    }

    val df = dataFrame
      .transform(dropCompoundKeyColumns())
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
    val structType = analyseSchema[T]

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
    * @param dataFrame
    * @return
    */
  def dropCompoundKeyColumns()(dataFrame: DataFrame): DataFrame = {
    // TODO do it safely
    dataFrame.drop(dataFrame.columns.filter(col => col.startsWith("_") && col.endsWith(compoundKeyName)): _*)
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
      .map(row => (row._1, row._2.sortBy(_.metadata.getStringArray(CompoundKey.toString())(1).toInt)
        .map(n => functions.col(n.name))))

    // For each element in keyColumns, add a new column to the input dataFrame
    keyColumns
      .foldLeft(dataFrame) {
        (df, col) =>
          df.withColumn(s"_${col._1}$compoundKeyName", functions.concat_ws(compoundKeySeparator, col._2: _*))
      }
  }

  /**
    * analyse Schema and define the structure file that have annotation key
    *
    * @tparam T
    * @return
    */
  def analyseSchema[T: ru.TypeTag]: StructType = {

    val classSymbol: ru.ClassSymbol = ru.symbolOf[T].asClass
    val paramListOfPrimaryConstructor = classSymbol.primaryConstructor.typeSignature.paramLists.head

    val sparkFields: List[StructField] = paramListOfPrimaryConstructor.map {
      field =>

        val schema = ScalaReflection.schemaFor(field.typeSignature)
        val sparkType = schema.dataType
        var nullable = schema.nullable

        // Black magic from here:
        // https://stackoverflow.com/questions/23046958/accessing-an-annotation-value-in-scala
        val annotations = field.annotations.collect {

          // Case where the field has annotation 'ColumnName'
          case columnName: ru.AnnotationApi if columnName.tree.tpe =:= ru.typeOf[ColumnName] =>
            val value = columnName.tree.children.tail.collectFirst {
              case ru.Literal(ru.Constant(name)) => name.toString
            }

            (ColumnName.toString(), Array(value.get))

          // Case where the field has annotation `CompoundKey`
          case compoundKey: ru.AnnotationApi if compoundKey.tree.tpe =:= ru.typeOf[CompoundKey] =>

            val attributes = Some(compoundKey.tree.children.tail.collect {
              case ru.Literal(ru.Constant(attribute)) => attribute.toString
            })

            // All compound key column should not be nullable
            nullable = false

            (CompoundKey.toString(), attributes.get.toArray)

        }.toMap

        val metadataBuilder = new MetadataBuilder()

        annotations.foreach(annoData => metadataBuilder.putStringArray(annoData._1, annoData._2))

        StructField(field.name.toString, sparkType, nullable, metadataBuilder.build())
    }

    StructType(sparkFields)
  }

}
