package com.jcdecaux.datacorp.spark.internal

import java.util.UUID

trait Identifiable {

  private[spark] val _uuid: UUID = UUID.randomUUID
  private[spark] val _name: String = getClass.getCanonicalName

  def getUUID: UUID = _uuid

  def getCanonicalName: String = _name

}
