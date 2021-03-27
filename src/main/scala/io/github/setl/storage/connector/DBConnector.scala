package io.github.setl.storage.connector

import io.github.setl.annotation.InterfaceStability
import io.github.setl.internal.{CanCreate, CanDelete, CanDrop}

@InterfaceStability.Evolving
abstract class DBConnector extends Connector
  with CanCreate
  with CanDrop
  with CanDelete {

}
