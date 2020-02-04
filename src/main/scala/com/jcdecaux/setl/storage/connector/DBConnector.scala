package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.internal.{CanCreate, CanDelete, CanDrop}

@InterfaceStability.Evolving
abstract class DBConnector extends Connector
  with CanCreate
  with CanDrop
  with CanDelete {

}
