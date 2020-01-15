package com.jcdecaux.setl.util

import java.util.Base64


object MermaidUtils {
  /**
    * Mermaid diagram code header for pretty print
    */
  val mermaidHeader = "--------- MERMAID DIAGRAM ---------"

  /**
    * Mermaid diagram code footer for pretty print
    */
  val mermaidFooter = "------- END OF MERMAID CODE -------"


  val summaryString = "You can copy the previous code to a markdown viewer that supports Mermaid."

  val liveEditorMessage = "Or you can try the live editor: "

  val linkPrefix = "https://mermaid-js.github.io/mermaid-live-editor/#/edit/"

  /**
    * Encode the Mermaid diagram to Base64
    *
    * @param mermaidDiagram Mermaid diagram code
    * @return the Base64 of the diagram code
    */
  def encodeMermaid(mermaidDiagram: String): String = {
    val jsonString = s"""{"code":"${mermaidDiagram.replace("\n", "\\n")}","mermaid":{"theme":"default"}}"""
    val encoded = Base64.getUrlEncoder.encode(jsonString.getBytes())

    new String(encoded)
  }

  /**
    * Message to be printed for live editor preview
    *
    * @param message Message before live editor preview link
    * @param prefix  link prefix
    * @param code    diagram base64 code
    * @return Full message for live editor preview
    */
  def mermaidDiagramLink(message: String, prefix: String, code: String): String = {
    message + prefix + code
  }

  /**
    * Format output of Mermaid diagram
    *
    * @param mermaidDiagram Mermaid diagram code
    * @return Pretty formatted output of Mermaid diagram with direct link
    */
  def printMermaid(mermaidDiagram: String): String = {
    val encoded = this.encodeMermaid(mermaidDiagram)
    val linkMessage = this.mermaidDiagramLink(this.liveEditorMessage, this.linkPrefix, encoded)

    mermaidHeader + "\n" + mermaidDiagram + "\n" + mermaidFooter + "\n\n" + summaryString + "\n\n" + linkMessage
  }
}