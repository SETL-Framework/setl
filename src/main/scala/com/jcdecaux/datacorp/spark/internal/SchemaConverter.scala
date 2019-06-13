package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotations.colName
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

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

  private[this] val alias: String = "alias"

  /**
    * Convert a dataframe to dataset according to the annotations
    *
    * @param df      input df
    * @param encoder implicit encoder of type T
    * @tparam T type of dataset
    * @return
    */
  def fromDF[T <: Product : ClassTag : ru.TypeTag](df: DataFrame)(implicit encoder: Encoder[T]): Dataset[T] = {

    val structType = analyseSchema[T]
    val changes = structType.filter(_.metadata.contains(alias)).map(x => x.name -> x.metadata.getString(alias)).toMap
    val newColumns = df.columns.map(x => changes.getOrElse(x, x))

    df.toDF(newColumns: _*).as[T]
  }

  /**
    * Convert a dataset to a dataframe according to [[colName]] annotations
    *
    * @param ds input dataset
    * @tparam T type of dataset
    * @return
    */
  def toDF[T <: Product : ClassTag : ru.TypeTag](ds: Dataset[T]): DataFrame = {

    val structType = analyseSchema[T]
    val changes = structType.filter(_.metadata.contains(alias)).map(x => x.metadata.getString(alias) -> x.name).toMap

    val newColumns = ds.toDF().columns.map(x => changes.getOrElse(x, x))

    ds.toDF(newColumns: _*)
  }

  private[this] def analyseSchema[T <: Product : ClassTag : ru.TypeTag]: StructType = {

    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)

    val classObj = scala.reflect.classTag[T].runtimeClass
    val classSymbol = runtimeMirror.classSymbol(classObj)

    val sparkFields = classSymbol.primaryConstructor.typeSignature.paramLists.head.map(field => {
      val sparkType = ScalaReflection.schemaFor(field.typeSignature).dataType

      // Black magic from here:
      // https://stackoverflow.com/questions/23046958/accessing-an-annotation-value-in-scala
      val attrName = field.annotations.collectFirst({
        case ann: ru.AnnotationApi if ann.tree.tpe =:= ru.typeOf[colName] =>
          ann.tree.children.tail.collectFirst({
            case ru.Literal(ru.Constant(name: String)) => name
          })
      }).flatten

      if (attrName.isDefined) {
        val metadata = new MetadataBuilder().putString(alias, field.name.toString).build()
        StructField(attrName.get, sparkType, nullable = true, metadata)
      } else {
        StructField(field.name.toString, sparkType, nullable = true, Metadata.empty)
      }
    })

    StructType(sparkFields)
  }
}
