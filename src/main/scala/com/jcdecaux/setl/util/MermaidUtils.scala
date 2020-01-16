package com.jcdecaux.setl.util

import java.util.Base64
import org.json4s.jackson.Serialization

private[setl] object MermaidUtils {

  /**
    * Needed to convert a Map object into JSON String
    */
  implicit val formats: org.json4s.DefaultFormats.type = org.json4s.DefaultFormats

  /**
    * Mermaid diagram code header for pretty print
    */
  val mermaidHeader = "--------- START OF MERMAID DIAGRAM ---------"

  /**
    * Mermaid diagram code footer for pretty print
    */
  val mermaidFooter = "---------- END OF MERMAID DIAGRAM ----------"

  val summaryString = "You can copy the previous code to a markdown viewer that supports Mermaid."

  val liveEditorMessage = "Otherwise you can try the live editor: "

  val linkPrefix = "https://mermaid-js.github.io/mermaid-live-editor/#/edit/"

  /**
    * Encode the Mermaid diagram to Base64
    *
    * @param mermaidDiagram Mermaid diagram code
    * @return the Base64 of the diagram code
    */
  def encodeMermaid(mermaidDiagram: String): String = {
    val mermaidMap = Map("code" -> mermaidDiagram, "mermaid" -> Map("theme" -> "default"))
    val jsonString = Serialization.write(mermaidMap)
    val encoded = Base64.getUrlEncoder.encode(jsonString.getBytes())
    new String(encoded).replace("\r", "")
  }

  /**
    * Message to be printed for live editor preview
    *
    * @param code    diagram base64 code
    * @return Full message for live editor preview
    */
  def mermaidDiagramLink(code: String): String = {
    this.liveEditorMessage + this.linkPrefix + code
  }

  /**
    * Format output of Mermaid diagram
    *
    * @param mermaidDiagram Mermaid diagram code
    * @return Pretty formatted output of Mermaid diagram with direct link
    */
  def printMermaid(mermaidDiagram: String): Unit = {
    val encoded = this.encodeMermaid(mermaidDiagram)
    val linkMessage = this.mermaidDiagramLink(encoded)

    println(
      s"""$mermaidHeader
         |$mermaidDiagram
         |$mermaidFooter
         |
         |$summaryString
         |
         |$linkMessage
         |""".stripMargin
    )
  }

}
