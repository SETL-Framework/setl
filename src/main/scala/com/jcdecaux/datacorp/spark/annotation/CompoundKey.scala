package com.jcdecaux.datacorp.spark.annotation

import scala.annotation.StaticAnnotation

/**
  * Mark current field as a part of a compound key. All the compound keys will be concatenated with a separator
  * The position of the current field could be set with the position argument
  *
  * @param position String, "1", "2", etc.
  */
@InterfaceStability.Stable
final case class CompoundKey(position: String) extends StaticAnnotation
