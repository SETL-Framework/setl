package com.jcdecaux.setl.annotation

import scala.annotation.StaticAnnotation

/**
 * Mark current field as a part of a compound key. All the compound keys will be concatenated with a separator
 * The position of the current field could be set with the position argument
 *
 * @param id       String, "primary", "sort", etc.
 * @param position String, "1", "2", etc.
 */
@InterfaceStability.Stable
final case class CompoundKey(id: String, position: String) extends StaticAnnotation

private[setl] object CompoundKey {
  private[this] val separator: String = "!@"
  import scala.reflect.runtime.{universe => ru}

  /** To be used to handle the scala reflect annotation api of compound key */
  def serialize(compoundKey: ru.AnnotationApi): String = {
    val attributes = compoundKey.tree.children.tail.collect {
      case ru.Literal(ru.Constant(attribute)) => attribute.toString
    }
    attributes.mkString(separator)
  }

  /** Deserialize the string of compound key into an object of CompoundKey */
  def deserialize(str: String): CompoundKey = {
    val data = str.split(separator)
    CompoundKey(data(0), data(1))
  }
}