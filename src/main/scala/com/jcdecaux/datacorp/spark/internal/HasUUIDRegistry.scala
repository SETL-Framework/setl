package com.jcdecaux.datacorp.spark.internal

import java.util.UUID

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

import scala.collection.mutable

/**
 * HasUUIDRegistry provide a UUID registry and methods to check if an
 * [[com.jcdecaux.datacorp.spark.internal.Identifiable]] object already
 * exists in its registry
 */
@InterfaceStability.Evolving
trait HasUUIDRegistry {

  /**
   * Registry is a HashSet that keeps the UUID of identifiable objects
   */
  private[this] val registry: mutable.HashSet[UUID] = mutable.HashSet()

  /**
   * Register a new [[com.jcdecaux.datacorp.spark.internal.Identifiable]] in registry
   *
   * @param item an object that inherit [[com.jcdecaux.datacorp.spark.internal.Identifiable]]
   * @return true if the given item is registered, false otherwise
   */
  def registerNewItem(item: Identifiable): Boolean = registerNewItem(item.getUUID)

  /**
   * Register a new UUID in registry
   *
   * @param uuid an UUID
   * @return true if the given UUID is registered, false otherwise
   */
  def registerNewItem(uuid: UUID): Boolean = if (registry.add(uuid)) true else false

  /**
   * Check if the Identifiable exists in the registry
   *
   * @param item an object that inherit [[com.jcdecaux.datacorp.spark.internal.Identifiable]]
   * @return true if it already exists in the registry, false otherwise
   */
  def has(item: Identifiable): Boolean = this.has(item.getUUID)

  /**
   * Check if the UUID exists in the registry
   *
   * @param uuid an UUID
   * @return true if it already exists in the registry, false otherwise
   */
  def has(uuid: UUID): Boolean = registry.contains(uuid)
}
