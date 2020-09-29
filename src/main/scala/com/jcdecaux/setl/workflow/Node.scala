package com.jcdecaux.setl.workflow

import java.util.UUID

import com.jcdecaux.setl.exception.InvalidDeliveryException
import com.jcdecaux.setl.internal.{HasDescription, HasDiagram, Identifiable, Logging}
import com.jcdecaux.setl.transformation._
import com.jcdecaux.setl.util.ReflectUtils

import scala.language.existentials
import scala.reflect.runtime

/**
 * Node is a representation of Factory in the DAG. One node could have multiple inputs and one single output.
 *
 * @param factoryClass class of the represented Factory
 * @param factoryUUID  UUID of the represented Factory
 * @param stage        stage where the node is located in the DAG
 * @param setters      setter's metadata
 * @param output       output of node
 */
private[workflow] case class Node(factoryClass: Class[_ <: Factory[_]],
                                  factoryUUID: UUID,
                                  stage: Int,
                                  setters: List[FactoryDeliveryMetadata],
                                  output: FactoryOutput)
  extends Identifiable
    with Logging
    with HasDescription
    with HasDiagram {

  def this(factory: Factory[_], stage: Int, finalNode: Boolean) {
    this(
      factoryClass = factory.getClass,
      factoryUUID = factory.getUUID,
      stage = stage,
      setters = FactoryDeliveryMetadata.builder().setFactory(factory).getOrCreate().toList,
      output = FactoryOutput(
        factory.deliveryType(),
        factory.consumers,
        factory.deliveryId,
        finalNode
        // this constructor will never be used to generate an external node
      )
    )
  }

  override def getPrettyName: String = ReflectUtils.getPrettyName(factoryClass)

  def input: List[FactoryInput] = setters.flatMap(s => s.getFactoryInputs)

  def listInputProducers: List[Class[_]] = this.input.map(_.producer)

  def findInputByType(t: runtime.universe.Type, deliveryId: String): List[FactoryInput] = {
    input.filter {
      i => i.runtimeType == t && i.deliveryId == deliveryId
    }
  }

  private[this] def formatDeliveryId(id: String): String = {
    if (id != Deliverable.DEFAULT_ID) {
      s" (delivery id: $id)"
    } else {
      ""
    }
  }

  override def describe(): this.type = {
    println(s"Factory : $getPrettyName")
    println(s"Stage   : $stage")
    input.foreach {
      i =>
        val deliveryId: String = formatDeliveryId(i.deliveryId)
        println(s"Input   : ${ReflectUtils.getPrettyName(i.runtimeType)}$deliveryId")
    }
    println(s"Output  : ${ReflectUtils.getPrettyName(output.runtimeType)}${formatDeliveryId(output.deliveryId)}") //
    println("----------------------------------------------------------")
    this
  }

  /**
   * For a new Node, return true if it is a target node of the current node, false otherwise
   *
   * @param next another [[Node]]
   */
  def targetNode(next: Node): Boolean = {

    val validNodeUUID = if (this.getUUID == next.getUUID) {
      logDebug("The two nodes have the same UUID")
      false
    } else {
      true
    }

    val validClassUUID = if (this.factoryUUID == next.factoryUUID) {
      logDebug("The two nodes are representing one same factory")
      false
    } else {
      true
    }

    val validStage = if (this.stage >= next.stage) {
      logDebug("The two nodes are in the same stage")
      false
    } else {
      true
    }

    val filteredInputs = next.findInputByType(this.output.runtimeType, this.output.deliveryId)

    val validTarget = filteredInputs.length match {
      case 0 => false
      case 1 => handleOneSingleMatchedInput(filteredInputs.head, next) // Found only one matching type
      case _ => handleMultipleMatchedInputs(filteredInputs, next) // Multiple variables of the same type were found in the next node
    }

    validStage && validTarget && validNodeUUID && validClassUUID
  }

  private[this] def handleOneSingleMatchedInput(matchedInput: FactoryInput, nextNode: Node): Boolean = {

    var validConsumer: Boolean = false
    var validProducer: Boolean = false

    // producer is valid if the input producer is not set or the input producer is the current node
    if (matchedInput.producer == classOf[External] || matchedInput.producer == this.factoryClass) {
      validProducer = true
    }

    if (this.output.consumer.isEmpty || this.output.consumer.contains(nextNode.factoryClass)) {
      validConsumer = true
    }

    validConsumer && validProducer
  }

  private[this] def handleMultipleMatchedInputs(matchedInputs: List[FactoryInput], nextNode: Node): Boolean = {

    var validConsumer: Boolean = false
    var validProducer: Boolean = false

    if (this.output.consumer.isEmpty || this.output.consumer.contains(nextNode.factoryClass)) {
      validConsumer = true
    }

    // if there exists some filteredInputs whose explicitly defined producer matches the class of the current node.
    val exactProducerMatch = matchedInputs.exists(_.producer == this.factoryClass)

    val nonExplicitlyDefinedProducers = matchedInputs.filter(_.producer == classOf[External])

    if (!exactProducerMatch && nonExplicitlyDefinedProducers.length > 1) {
      throw new InvalidDeliveryException(s"Multiple inputs in ${nextNode.getPrettyName} are " +
        s"of type ${this.output.runtimeType.toString}. You may have to explicitly declare their producer " +
        s"in the Delivery annotation, otherwise this may cause unexpected pipeline result.")
    }

    validProducer = exactProducerMatch || nonExplicitlyDefinedProducers.length == 1

    validConsumer && validProducer
  }

  /** Get the diagram ID */
  override def diagramId: String = this.getPrettyName.replaceAll("[\\[\\]]", "_")

  /** Generate the diagram */
  override def toDiagram: String = {

    val fields = this.input.map {
      i => s"+${ReflectUtils.getPrettyName(i.runtimeType)}"
    }.mkString("\n  ")

    val thisDiagramString =
      s"""class ${this.diagramId} {
         |  <<Factory[${ReflectUtils.getPrettyName(this.output.runtimeType)}]>>
         |  $fields
         |}
         |""".stripMargin

    val link = s"${this.output.diagramId} <|.. ${this.diagramId} : Output"

    s"""$thisDiagramString
       |${this.output.toDiagram}
       |$link
       |""".stripMargin
  }

}
