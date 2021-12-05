package io.github.setl.workflow

import io.github.setl.internal.{HasDescription, HasDiagram}
import io.github.setl.transformation.Deliverable
import io.github.setl.util.ReflectUtils

import scala.reflect.runtime

/**
 * Flow is a representation of the data transfer in a Pipeline.
 *
 * @param from origin node of the transfer
 * @param to   destination node of the transfer
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
    s"${to.diagramId} <|-- ${from.output.diagramId} : Input".stripMargin
  }

  override def diagramId: String = ""
}
