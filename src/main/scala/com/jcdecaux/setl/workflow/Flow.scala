package com.jcdecaux.setl.workflow

import com.jcdecaux.setl.internal.{HasDescription, HasDiagram}
import com.jcdecaux.setl.transformation.Deliverable
import com.jcdecaux.setl.util.ReflectUtils

import scala.reflect.runtime

/**
 * Flow is a representation of the data transfer in a Pipeline.
 *
 * @param from    origin node of the transfer
 * @param to      destination node of the transfer
 */
private[workflow] case class Flow(from: Node, to: Node) extends HasDescription with HasDiagram {

  def payload: runtime.universe.Type = from.output.runtimeType

  def stage: Int = from.stage

  def deliveryId: String = from.output.deliveryId

  override def describe(): this.type = {
    if (deliveryId != Deliverable.DEFAULT_ID) {
      println(s"Delivery id : $deliveryId")
    }
    println(s"Stage       : $stage")
    println(s"Direction   : ${from.getPrettyName} ==> ${to.getPrettyName}")
    println(s"PayLoad     : ${ReflectUtils.getPrettyName(payload)}")
    println("----------------------------------------------------------")
    this
  }

  override def toDiagram: String = {
    if (from.factoryClass == classOf[External]) {
      s"${to.diagramId} <|-- ${from.output.diagramId}External : Input"
    } else {
      s"${to.diagramId} <|-- ${from.output.diagramId} : Input"
    }
  }

  override def diagramId: String = ""
}
