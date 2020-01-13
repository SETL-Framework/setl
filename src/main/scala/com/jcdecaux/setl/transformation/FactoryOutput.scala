package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.internal.{HasDiagram, HasType}
import com.jcdecaux.setl.util.ReflectUtils
import org.apache.spark.sql.Dataset

import scala.reflect.runtime

private[setl] case class FactoryOutput(override val runtimeType: runtime.universe.Type,
                                       consumer: Seq[Class[_ <: Factory[_]]],
                                       deliveryId: String = Deliverable.DEFAULT_ID,
                                       finalOutput: Boolean = false) extends HasType with HasDiagram {

  override def diagramId: String = {
    val finalSuffix = if (finalOutput) {
      "final"
    } else {
      ""
    }
    ReflectUtils.getPrettyName(this.runtimeType).replaceAll("[\\[\\]]", "_") + deliveryId + finalSuffix
  }

  override def toDiagram: String = {

    val isDataset = this.runtimeType.baseClasses.head.asClass == runtime.universe.symbolOf[Dataset[_]].asClass

    val fields = if (isDataset) {
      val constructorArgList = this.runtimeType
        .typeArgs.head
        .baseClasses.head
        .asClass
        .primaryConstructor
        .typeSignature
        .paramLists
        .head

      constructorArgList.map {
        i => s"+${i.name}: ${ReflectUtils.getPrettyName(i.typeSignature)}"
      }.mkString("\n  ")

    } else {
      ""
    }

    s"""class ${this.diagramId} {
       |  <<${ReflectUtils.getPrettyName(this.runtimeType)}>>
       |  $fields
       |}
       |""".stripMargin
  }
}
