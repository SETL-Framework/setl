package com.jcdecaux.setl.transformation

import com.jcdecaux.setl.internal.{HasDiagram, HasType}
import com.jcdecaux.setl.util.ReflectUtils

import scala.reflect.runtime

private[setl] case class FactoryOutput(override val runtimeType: runtime.universe.Type,
                                       consumer: Seq[Class[_ <: Factory[_]]],
                                       deliveryId: String = Deliverable.DEFAULT_ID) extends HasType with HasDiagram {
  override def toDiagram: String = {

    val output = this.runtimeType
      .baseClasses
      .head
      .asClass
      .primaryConstructor
      .typeSignature
      .paramLists
      .head
      .map {
        x => x.name + " = " + ReflectUtils.getPrettyName(x.typeSignature)
      }

    output.foreach(println)

    println(ReflectUtils.getPrettyName(this.runtimeType))

    println(this.runtimeType.baseClasses.head.asClass.fullName == "org.apache.spark.sql.Dataset")

    ""
  }
}
