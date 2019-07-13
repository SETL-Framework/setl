package com.jcdecaux.datacorp.spark.internal

import java.util.UUID

trait Identifiable {

  private[spark] val _uuid: String = UUID.randomUUID.toString
  private[spark] val _name: String = getClass.getCanonicalName

  def getUUID: String = _uuid

  def getCanonicalName: String = _name

}
