package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.internal.{CanDelete, CanDrop, CanUpdate, CanVacuum}

@InterfaceStability.Evolving
abstract class ACIDConnector extends Connector
  with CanUpdate
  with CanDrop
  with CanDelete
  with CanVacuum {

}
