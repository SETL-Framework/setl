package com.jcdecaux.setl.internal

import java.util.UUID

import com.jcdecaux.setl.annotation.InterfaceStability
import com.jcdecaux.setl.exception.AlreadyExistsException

import scala.collection.immutable.ListMap

/**
 * HasUUIDRegistry provide a UUID registry and methods to check if an
 * [[com.jcdecaux.setl.internal.Identifiable]] object already
 * exists in its registry
 */
@InterfaceStability.Evolving
trait HasRegistry[T <: Identifiable] {

  /**
   * Registry is a HashSet that keeps the UUID of identifiable objects
   */
  private[this] var registry: ListMap[UUID, T] = ListMap.empty

  /**
   * Register a new [[com.jcdecaux.setl.internal.Identifiable]] in registry
   *
   * @param item an object that inherit [[com.jcdecaux.setl.internal.Identifiable]]
   * @return true if the given item is registered, false otherwise
   */
  @throws[AlreadyExistsException]
  protected def registerNewItem(item: T): Unit = {
    if (hasRegisteredItem(item)) {
      throw new AlreadyExistsException(s"The current item ${item.getUUID} of type ${item.getCanonicalName} already exists")
    } else {
      registry += (item.getUUID -> item)
    }
  }

  /** Clear the registry */
  protected def clearRegistry(): Unit = {
    registry = ListMap.empty
  }

  /**
   * Register multiple items
   * @param items an [[com.jcdecaux.setl.internal.Identifiable]] object
   */
  protected def registerNewItems(items: Iterable[T]): Unit = items.foreach(this.registerNewItem)

  /**
   * Check if the Identifiable exists in the registry
   *
   * @param item an object that inherit [[com.jcdecaux.setl.internal.Identifiable]]
   * @return true if it already exists in the registry, false otherwise
   */
  def hasRegisteredItem(item: Identifiable): Boolean = this.hasRegisteredItem(item.getUUID)

  /**
   * Check if the UUID exists in the registry
   *
   * @param uuid an UUID
   * @return true if it already exists in the registry, false otherwise
   */
  def hasRegisteredItem(uuid: UUID): Boolean = registry.contains(uuid)

  /** Return the registry */
  def getRegistry: ListMap[UUID, T] = this.registry

  /**
   * For a given UUID, return the corresponding registered item
   * @param uuid uuid
   * @return
   */
  def getRegisteredItem(uuid: UUID): Option[T] = registry.get(uuid)

  /** Return the number of items in the current registry */
  def getRegistryLength: Long = registry.size

  /** Return true if the registry is empty, false otherwise */
  def isRegistryEmpty: Boolean = registry.isEmpty

  /**
   * Return the last registered item
   * @return if the registry is empty, None will be returned
   */
  def lastRegisteredItem: Option[T] = if (isRegistryEmpty) {
      None
    } else {
      Option(registry.last._2)
    }

}
