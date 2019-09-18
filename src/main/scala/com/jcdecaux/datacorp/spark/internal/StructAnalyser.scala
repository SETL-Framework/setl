package com.jcdecaux.datacorp.spark.internal

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

import scala.reflect.runtime.{universe => ru}

object StructAnalyser {

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
