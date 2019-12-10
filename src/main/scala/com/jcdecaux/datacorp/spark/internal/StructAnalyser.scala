package com.jcdecaux.datacorp.spark.internal

import java.lang.annotation.Annotation

import com.jcdecaux.datacorp.spark.annotation.{ColumnName, CompoundKey, Compress}
import com.jcdecaux.datacorp.spark.storage.Compressor
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

import scala.reflect.runtime.{universe => ru}

object StructAnalyser {

  /**
   * Analyse the metadata of the generic type T. Fetch information for its annotated fields.
   *
   * @tparam T Type that we'd like to analyse
   * @return a StructType object containing information of each field.
   */
  def analyseSchema[T: ru.TypeTag]: StructType = {

    val classSymbol: ru.ClassSymbol = ru.symbolOf[T].asClass
    val paramListOfPrimaryConstructor = classSymbol.primaryConstructor.typeSignature.paramLists.head

    val columnToBeCompressed = findCompressColumn[T]

    val sparkFields: List[StructField] = paramListOfPrimaryConstructor.zipWithIndex.map {
      case (field, index) =>

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
            (ColumnName.toString(), Array(value.get)) // (ColumnName, ["alias"])

          // Case where the field has annotation `CompoundKey`
          case compoundKey: ru.AnnotationApi if compoundKey.tree.tpe =:= ru.typeOf[CompoundKey] =>
            val attributes = Some(compoundKey.tree.children.tail.collect {
              case ru.Literal(ru.Constant(attribute)) => attribute.toString
            })
            // All compound key column should not be nullable
            nullable = false
            (CompoundKey.toString(), attributes.get.toArray) // (ColumnName, ["id", "position"])

          case compress: ru.AnnotationApi if compress.tree.tpe =:= ru.typeOf[Compress] =>
            val compressor = columnToBeCompressed.find(_._1 == index).get._2.getCanonicalName
            (classOf[Compress].getCanonicalName, Array(compressor)) // (com.jcdecaux.datacorp.spark.xxxx, ["compressor_canonical_name"])

        }.toMap

        val metadataBuilder = new MetadataBuilder()
        annotations.foreach(annoData => metadataBuilder.putStringArray(annoData._1, annoData._2))

        StructField(field.name.toString, sparkType, nullable, metadataBuilder.build())
    }

    StructType(sparkFields)
  }

  /**
   * Inspect the class T, find fields with @Compress annotation
   *
   * @tparam T type of case class to be inspected
   * @return an array of (index of the field, and class of compressor)
   */
  private[this] def findCompressColumn[T: ru.TypeTag]: Array[(Int, Class[_ <: Compressor])] = {

    val classTag = ru.typeTag[T].mirror.runtimeClass(ru.typeTag[T].tpe)
    val paramAnnotations = classTag.getConstructors.head.getParameterAnnotations

    // Get the index and the class of compressor if the parameter needs to be compressed
    paramAnnotations.zipWithIndex.collect {
      case (anno: Array[Annotation], index: Int) if anno.exists(_.isInstanceOf[Compress]) =>
        val compressAnnotation = anno.find(_.isInstanceOf[Compress]).get.asInstanceOf[Compress]
        val compressorMethod = compressAnnotation.annotationType().getDeclaredMethod("compressor")
        val compressor = compressorMethod.invoke(compressAnnotation).asInstanceOf[Class[_ <: Compressor]]
        (index, compressor)
    }
  }

}
