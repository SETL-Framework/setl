package io.github.setl.storage.connector

import io.github.setl.annotation.InterfaceStability
import io.github.setl.internal.{CanDelete, CanDrop, CanUpdate, CanVacuum}

@InterfaceStability.Evolving
abstract class ACIDConnector extends Connector
  with CanUpdate
  with CanDrop
  with CanDelete
  with CanVacuum {

}
