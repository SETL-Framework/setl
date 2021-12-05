package io.github.setl.internal

import java.lang.annotation.Annotation

import io.github.setl.annotation.{ColumnName, CompoundKey, Compress}
import io.github.setl.storage.Compressor
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

import scala.reflect.runtime.{universe => ru}

/**
 * StructAnalyser will analyse the schema for a given case class. It will register the information about
 */
object StructAnalyser extends Logging {

  private[setl] val COMPOUND_KEY: String = classOf[CompoundKey].getCanonicalName
  private[setl] val COLUMN_NAME: String = classOf[ColumnName].getCanonicalName
  private[setl] val COMPRESS: String = classOf[Compress].getCanonicalName

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
        val dataType = schema.dataType
        var nullable = schema.nullable

        // Black magic from here:
        // https://stackoverflow.com/questions/23046958/accessing-an-annotation-value-in-scala
        val annotations = field.annotations.collect {

          // Case where the field has the annotation 'ColumnName'
          case columnName: ru.AnnotationApi if columnName.tree.tpe =:= ru.typeOf[ColumnName] =>

            val value = columnName.tree.children.tail.collectFirst {
              case ru.Literal(ru.Constant(name)) => name.toString
            }
            (COLUMN_NAME, Array(value.get)) // (ColumnName, ["alias"])

          // Case where the field has the annotation `CompoundKey`
          case compoundKey: ru.AnnotationApi if compoundKey.tree.tpe =:= ru.typeOf[CompoundKey] =>

            val attribute = CompoundKey.serialize(compoundKey)
            nullable = false // All compound key column should not be nullable
            (COMPOUND_KEY, Array(attribute)) // (ColumnName, ["id", "position"])

          // Case where the field has the annotation `Compress`
          case compress: ru.AnnotationApi if compress.tree.tpe =:= ru.typeOf[Compress] =>
            val compressor = columnToBeCompressed.find(_._1 == index).get._2.getCanonicalName
            (COMPRESS, Array(compressor)) // (io.github.setl.xxxx, ["compressor_canonical_name"])

        }
          .groupBy(_._1)
          .map {
            case (group, elements) => (group, elements.flatMap(_._2))
          }

        val metadataBuilder = new MetadataBuilder()
        annotations.foreach {
          annotationData =>
            verifyAnnotation(annotationData._1, annotationData._2)
            metadataBuilder.putStringArray(annotationData._1, annotationData._2.toArray)
        }

        StructField(field.name.toString, dataType, nullable, metadataBuilder.build())
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

  /**
   * Inspect the class T, find fields with @COMPOUND_KEY annotation
   *
   * @tparam T type of case class to be inspected
   * @return an array of (column names)
   */
  def findCompoundColumns[T: ru.TypeTag]: Seq[String] = {
    this.analyseSchema[T].filter(_.metadata.contains(COMPOUND_KEY))
      .map(x => if(!x.metadata.contains(COLUMN_NAME)) x.name else x.metadata.getStringArray(COLUMN_NAME)(0))
  }

  /**
   * Verify the annotations of a field with the following conditions:
   *
   * <p>A field should not have more than one @ColumnName annotation</p>
   * <p>A field should not have more than one @Compress annotation</p>
   *
   * @param annotation name of the annotation
   * @param data       attribute of the annotation
   */
  private[this] def verifyAnnotation(annotation: String, data: List[String]): Unit = {
    if (annotation == COLUMN_NAME) {
      require(data.length == 1, "There should not be more than one ColumnName annotation")
    } else if (annotation == COMPRESS) {
      require(data.length == 1, "There should not be more than one Compress annotation")
    }
  }

}
