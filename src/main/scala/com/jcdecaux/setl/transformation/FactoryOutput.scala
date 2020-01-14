package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.internal.{HasDiagram, HasType}
import com.jcdecaux.setl.util.ReflectUtils
import org.apache.spark.sql.Dataset

import scala.reflect.runtime

private[setl] case class FactoryOutput(override val runtimeType: runtime.universe.Type,
                                       consumer: Seq[Class[_ <: Factory[_]]],
                                       deliveryId: String = Deliverable.DEFAULT_ID,
                                       finalOutput: Boolean = false,
                                       external: Boolean = false) extends HasType with HasDiagram {

  override def diagramId: String = {
    val finalSuffix = if (finalOutput) "Final" else ""

    val externalSuffix = if (external) "External" else ""

    super.formatDiagramId(ReflectUtils.getPrettyName(runtimeType), deliveryId, finalSuffix + externalSuffix)
  }

  private[this] val typeToExclude = List(
    "String", "Double", "Int", "Float", "Long"
  )

  private[this] def payloadField: List[String] = {
    val isDataset = this.runtimeType.baseClasses.head.asClass == runtime.universe.symbolOf[Dataset[_]].asClass

    val isNative = {
      val runtimeTypeName = runtimeType.toString
      typeToExclude.contains(runtimeTypeName) || runtimeTypeName.startsWith("scala") || runtimeTypeName.startsWith("java")
    }

    if (isDataset) {
      val datasetTypeArgFields = super.getTypeArgList(this.runtimeType.typeArgs.head)
      datasetTypeArgFields.map {
        i => s">${i.name}: ${ReflectUtils.getPrettyName(i.typeSignature)}"
      }

    } else if (!isNative) {
      val typeArgFields = super.getTypeArgList(this.runtimeType)
      typeArgFields.map {
        i => s"-${i.name}: ${ReflectUtils.getPrettyName(i.typeSignature)}"
      }

    } else {
      List.empty
    }
  }

  override def toDiagram: String = {

    val fields = this.payloadField.mkString("\n  ")

    s"""class ${this.diagramId} {
       |  <<${ReflectUtils.getPrettyName(this.runtimeType)}>>
       |  $fields
       |}
       |""".stripMargin
  }
}
