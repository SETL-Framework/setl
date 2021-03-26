package io.github.setl.workflow

import java.util.UUID

import io.github.setl.transformation.Factory

sealed abstract class External private extends Factory[External]

/**
 * Singleton for external data source
 */
object External {
  val NODE: Node = Node(
    classOf[External],
    UUID.fromString("00000000-0000-0000-0000-000000000000"),
    -1,
    List(),
    null
  )
}
