package com.jcdecaux.setl.internal

import java.util.UUID

import com.jcdecaux.setl.annotation.InterfaceStability

/**
 * Identifiable generates an UUID for any object that implement the trait
 */
@InterfaceStability.Evolving
trait Identifiable {

  private[this] val _uuid: UUID = UUID.randomUUID

  private[this] val _name: String = getClass.getCanonicalName

  def getUUID: UUID = _uuid

  def getCanonicalName: String = _name

}
