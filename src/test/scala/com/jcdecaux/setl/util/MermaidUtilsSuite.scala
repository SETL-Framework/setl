package com.jcdecaux.setl.util

import java.io.ByteArrayOutputStream
import org.scalatest.funsuite.AnyFunSuite

class MermaidUtilsSuite extends AnyFunSuite{

  import MermaidUtils._

  test("EncodeMermaidDiagram") {
    // Use dropRight because encoding adds "==" at the end
    val encoded = encodeMermaid(MermaidUtilsSuite.diagram).dropRight(2)

    val expectedCode = "eyJjb2RlIjoiY2xhc3NEaWFncmFtXG5DbGFzczAxIDx8LS0gQXZlcnlMb25nQ2xhc3MgOiBDb29sXG48PGludGVyZmFjZT4-IENsYXNzMDFcbkNsYXNzMDkgLS0-IEMyIDogV2hlcmUgYW0gaT9cbkNsYXNzMDkgLS0qIEMzXG5DbGFzczA5IC0tfD4gQ2xhc3MwN1xuQ2xhc3MwNyA6IGVxdWFscygpXG5DbGFzczA3IDogT2JqZWN0W10gZWxlbWVudERhdGFcbkNsYXNzMDEgOiBzaXplKClcbkNsYXNzMDEgOiBpbnQgY2hpbXBcbkNsYXNzMDEgOiBpbnQgZ29yaWxsYVxuY2xhc3MgQ2xhc3MxMCB7XG4gID4-c2VydmljZT4-XG4gIGludCBpZFxuICBzaXplKClcbn0iLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCJ9fQ"

    assert(encoded == expectedCode)
  }

  test("GenerateLinkMessage") {
    val code = "base64code"
    val linkMessage = mermaidDiagramLink(code)

    val expectedLinkMessage = "Otherwise you can try the live editor: https://mermaid-js.github.io/mermaid-live-editor/#/edit/base64code"

    assert(linkMessage == expectedLinkMessage)
  }

  test("MermaidURLOutput") {
    val output = new ByteArrayOutputStream
    Console.withOut(output)(printMermaid(MermaidUtilsSuite.diagram))

    val expectedOutput =
      """--------- START OF MERMAID DIAGRAM ---------
        |classDiagram
        |Class01 <|-- AveryLongClass : Cool
        |<<interface>> Class01
        |Class09 --> C2 : Where am i?
        |Class09 --* C3
        |Class09 --|> Class07
        |Class07 : equals()
        |Class07 : Object[] elementData
        |Class01 : size()
        |Class01 : int chimp
        |Class01 : int gorilla
        |class Class10 {
        |  >>service>>
        |  int id
        |  size()
        |}
        |---------- END OF MERMAID DIAGRAM ----------
        |
        |You can copy the previous code to a markdown viewer that supports Mermaid.
        |
        |Otherwise you can try the live editor: https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoiY2xhc3NEaWFncmFtXG5DbGFzczAxIDx8LS0gQXZlcnlMb25nQ2xhc3MgOiBDb29sXG48PGludGVyZmFjZT4-IENsYXNzMDFcbkNsYXNzMDkgLS0-IEMyIDogV2hlcmUgYW0gaT9cbkNsYXNzMDkgLS0qIEMzXG5DbGFzczA5IC0tfD4gQ2xhc3MwN1xuQ2xhc3MwNyA6IGVxdWFscygpXG5DbGFzczA3IDogT2JqZWN0W10gZWxlbWVudERhdGFcbkNsYXNzMDEgOiBzaXplKClcbkNsYXNzMDEgOiBpbnQgY2hpbXBcbkNsYXNzMDEgOiBpbnQgZ29yaWxsYVxuY2xhc3MgQ2xhc3MxMCB7XG4gID4-c2VydmljZT4-XG4gIGludCBpZFxuICBzaXplKClcbn0iLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCJ9fQ==""".stripMargin

    assert(output.toString.trim() == expectedOutput)
  }

}

object MermaidUtilsSuite {
  val diagram: String =
    """classDiagram
      |Class01 <|-- AveryLongClass : Cool
      |<<interface>> Class01
      |Class09 --> C2 : Where am i?
      |Class09 --* C3
      |Class09 --|> Class07
      |Class07 : equals()
      |Class07 : Object[] elementData
      |Class01 : size()
      |Class01 : int chimp
      |Class01 : int gorilla
      |class Class10 {
      |  >>service>>
      |  int id
      |  size()
      |}""".stripMargin
}
