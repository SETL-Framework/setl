package com.jcdecaux.setl.internal

import scala.reflect.runtime

trait HasDiagram {

  def toDiagram: String

  def diagramId: String

  protected def getTypeArgList(tpe: runtime.universe.Type): List[runtime.universe.Symbol] = {
    tpe
      .baseClasses.head
      .asClass
      .primaryConstructor
      .typeSignature
      .paramLists
      .head
  }

  protected def formatDiagramId(prettyName: String,
                                deliveryId: String,
                                suffix: String): String = {

    prettyName.replaceAll("[\\[\\]]", "") + deliveryId.capitalize + suffix

  }

}
