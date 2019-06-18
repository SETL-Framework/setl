package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotations.CompoundKey
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

/**
  * SchemaConverter will rename the column of a dataset/dataframe according to the given case class T.
  *
  * {{{
  *   import com.jcdecaux.datacorp.spark.annotations.DynamoDBKey
  *   case class MyObject(@DynamoDBKey("1") column1: String, @DynamoDBKey("2") column2: String)
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
  * +-------+-------+-----------+
  * |column1|column2|DynamoDBKey|
  * +-------+-------+-----------+
  * |      a|      A|        a-A|
  * |      b|      B|        b-B|
  * +-------+-------+-----------+
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
@deprecated
private[spark] object SchemaConverterKey {

  private[this] val alias: String = "DynamoDBKey"
  private[this] val Key: String = "key"

  /**
    * Convert a Table DynamoDb with Key to a datasets origine structor.
    * *   val df = SchemaConverter.toDF(ds)
    * * +-------+-------+-----------+
    * * |column1|column2|    key    |
    * * +-------+-------+-----------+
    * * |      a|      A|        a-A|
    * * |      b|      B|        b-B|
    * * +-------+-------+-----------+
    * *
    * *   val ds2 = SchemaConverter.fromDF[MyObject](df)
    * *   // +-------+-------+
    * *   // |column1|column2|
    * *   // +-------+-------+
    * *   // |      a|      A|
    * *   // |      b|      B|
    * *   // +-------+-------+
    * *
    * * }}}
    **
    * @param df
    * @param encoder
    * @tparam T
    * @return
    */

  def fromDF[T <: Product : ClassTag : ru.TypeTag](df: DataFrame)(implicit encoder: Encoder[T]): Dataset[T] = {

    val structType = analyseSchema[T, CompoundKey]
    val changes = structType.filter(_.metadata.contains(alias)).map(x => x.name -> x.metadata.getString(alias)).toMap

    val newColumns = df.columns.map(x => changes.getOrElse(x, x))
    df.toDF(newColumns: _*).drop(Key).as[T]
  }

  /**
    *  Convert a Dataset to Dataframe with Key annotation  to a DynamoDb table with key column
    * *   import com.jcdecaux.datacorp.spark.annotations.DynamoDBKey
    * *   case class MyObject(@DynamoDBKey("1") column1: String, @DynamoDBKey("2") column2: String)
    * *
    * *   val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
    * *   // +-------+-------+
    * *   // |column1|column2|
    * *   // +-------+-------+
    * *   // |      a|      A|
    * *   // |      b|      B|
    * *   // +-------+-------+
    * *
    * *   val df = SchemaConverter.toDF(ds)
    * * +-------+-------+-----------+
    * * |column1|column2|    key    |
    * * +-------+-------+-----------+
    * * |      a|      A|        a-A|
    * * |      b|      B|        b-B|
    * * +-------+-------+-----------+
    *
    * @param ds input dataset
    * @tparam T type of dataset
    * @return
    */
  def toDF[T <: Product : ClassTag : ru.TypeTag](ds: Dataset[T]): DataFrame = {

    val structType = analyseSchema[T, CompoundKey]

    val changes: Map[String, Int] = structType.filter(_.metadata.contains(alias)).map(x => x.metadata.getString(alias) -> x.name.toInt).toMap

    val sortedChanges: Seq[Column] = changes.toSeq.sortBy(_._2).map(row => col(row._1))

    ds.withColumn(Key, concat_ws("-", sortedChanges: _*))
  }


  /**
    * analyse Schema and define the structure file that have annotation key
    * @tparam T
    * @tparam A
    * @return
    */
  private[this] def analyseSchema[T <: Product : ClassTag : ru.TypeTag, A <: Product : ClassTag : ru.TypeTag]: StructType = {

    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)

    val classObj = scala.reflect.classTag[T].runtimeClass
    val classSymbol = runtimeMirror.classSymbol(classObj)

    val sparkFields = classSymbol.primaryConstructor.typeSignature.paramLists.head.map(field => {
      val sparkType = ScalaReflection.schemaFor(field.typeSignature).dataType

      // Black magic from here:
      // https://stackoverflow.com/questions/23046958/accessing-an-annotation-value-in-scala
      val attrName = field.annotations.collectFirst({
        case ann: ru.AnnotationApi if ann.tree.tpe =:= ru.typeOf[A] =>
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
