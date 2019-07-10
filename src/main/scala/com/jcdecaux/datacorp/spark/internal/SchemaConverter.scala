package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, functions}

import scala.reflect.ClassTag
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
private[spark] object SchemaConverter {

  private[this] val compoundKeyName: String = "_key"
  private[this] val compoundKeySeparator: String = "-"

  /**
    * Convert a dataframe to dataset according to the annotations
    *
    * @param dataFrame input df
    * @param encoder   implicit encoder of type T
    * @tparam T type of dataset
    * @return
    */
  def fromDF[T <: Product : ClassTag : ru.TypeTag](dataFrame: DataFrame)(implicit encoder: Encoder[T]): Dataset[T] = {
    val structType = analyseSchema[T]

    dataFrame
      .transform(handleCompoundKeyFromDF())
      .transform(handleColumnNameFromDF(structType))
      .as[T]
  }

  /**
    * Convert a dataset to a DataFrame according to annotations
    *
    * @param dataset input dataset
    * @tparam T type of dataset
    * @return
    */
  def toDF[T <: Product : ClassTag : ru.TypeTag](dataset: Dataset[T]): DataFrame = {
    val structType = analyseSchema[T]

    dataset
      .toDF()
      .transform(handleCompoundKeyToDF(structType))
      .transform(handleColumnNameToDF(structType))
  }

  /**
    * {{{
    *    import com.jcdecaux.datacorp.spark.annotations.ColumnName
    *
    *    case class MyObject(@ColumnName("col1") column1: String, column2: String)
    *
    *    convert
    *    +-------+-------+
    *    |column1|column2|
    *    +-------+-------+
    *    |      a|      A|
    *    |      b|      B|
    *    +-------+-------+
    *
    *    to
    *    +----+-------+
    *    |col1|column2|
    *    +----+-------+
    *    |   a|      A|
    *    |   b|      B|
    *    +----+-------+
    * }}}
    *
    * @param structType
    * @param dataFrame
    * @return
    */
  private[this] def handleColumnNameFromDF(structType: StructType)(dataFrame: DataFrame): DataFrame = {
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
  private[this] def handleColumnNameToDF(structType: StructType)(dataFrame: DataFrame): DataFrame = {
    val changes = structType
      .filter(_.metadata.contains(ColumnName.toString()))
      .map(x => x.name -> x.metadata.getStringArray(ColumnName.toString())(0))
      .toMap

    val newColumns = dataFrame.columns.map(columnName => changes.getOrElse(columnName, columnName))

    dataFrame.toDF(newColumns: _*)
  }

  /**
    * Drop any column called <code>_key</code>
    *
    * @param dataFrame
    * @return
    */
  private[this] def handleCompoundKeyFromDF()(dataFrame: DataFrame): DataFrame = {
    // TODO do it safely
    dataFrame.drop(dataFrame.columns.filter(col => col.startsWith("_") && col.endsWith(compoundKeyName)): _*)
  }

  /**
    * {{{
    *   import com.jcdecaux.datacorp.spark.annotations.CombinedKey
    *   case class MyObject(@CombinedKey("2") column1: String, @CombinedKey("1") column2: String)
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
    *   +-------+-------+-------+
    *   |column1|column2|   _key|
    *   +-------+-------+-------+
    *   |      a|      A|    A-a|
    *   |      b|      B|    B-b|
    *   +-------+-------+-------+
    * }}}
    *
    * @param structType
    * @param dataFrame
    * @return
    */
  private[this] def handleCompoundKeyToDF(structType: StructType)(dataFrame: DataFrame): DataFrame = {
    val keyColumns = structType
      .filter(_.metadata.contains(CompoundKey.toString()))
      .groupBy(_.metadata.getStringArray(CompoundKey.toString())(0))
      .map(row => (row._1, row._2.sortBy(_.metadata.getStringArray(CompoundKey.toString())(1).toInt)
        .map(n => functions.col(n.name))))

    var dataFrameWithKeys = dataFrame

    if (keyColumns.nonEmpty) {
      keyColumns.foreach(row => {
        dataFrameWithKeys = dataFrameWithKeys.withColumn("_" + row._1 + compoundKeyName, functions.concat_ws(compoundKeySeparator, row._2: _*))
      })
    }

    dataFrameWithKeys
  }

  /**
    * analyse Schema and define the structure file that have annotation key
    *
    * @tparam T
    * @return
    */
  private[this] def analyseSchema[T <: Product : ClassTag : ru.TypeTag]: StructType = {

    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)

    val classObj = scala.reflect.classTag[T].runtimeClass
    val classSymbol = runtimeMirror.classSymbol(classObj)

    val sparkFields = classSymbol.primaryConstructor.typeSignature.paramLists.head.map(field => {
      val sparkType = ScalaReflection.schemaFor(field.typeSignature).dataType

      // Black magic from here:
      // https://stackoverflow.com/questions/23046958/accessing-an-annotation-value-in-scala
      val annotations = field.annotations.collect({

        // Case where the field has annotation 'ColumnName'
        case columnName: ru.AnnotationApi if columnName.tree.tpe =:= ru.typeOf[ColumnName] =>
          val value = columnName.tree.children.tail.collectFirst({
            case ru.Literal(ru.Constant(name: String)) => name
          })

          (ColumnName.toString(), Array(value.get))

        // Case where the field has annotation `CompoundKey`
        case compoundKey: ru.AnnotationApi if compoundKey.tree.tpe =:= ru.typeOf[CompoundKey] =>

          val attributes = Some(compoundKey.tree.children.tail.collect({
            case ru.Literal(ru.Constant(attribute: String)) => attribute
          }))

          (CompoundKey.toString(), attributes.get.toArray)

      }).toMap

      val metadataBuilder = new MetadataBuilder()

      annotations.foreach({
        annotationData => metadataBuilder.putStringArray(annotationData._1, annotationData._2)
      })

      StructField(field.name.toString, sparkType, nullable = true, metadataBuilder.build())
    })

    StructType(sparkFields)
  }

}
