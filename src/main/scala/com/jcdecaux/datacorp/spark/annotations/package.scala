package com.jcdecaux.datacorp.spark

import scala.annotation.StaticAnnotation

package object annotations {

  /**
    * Define an alias for the current field in the table
    *
    * @param name alias of the current field name
    */
  final case class ColumnName(name: String) extends StaticAnnotation

  /**
    * Mark current field as a part of a compound key. All the compound keys will be concatenated with a separator
    * The position of the current field could be set with the position argument
    *
    * @param position String, "1", "2", etc.
    */
  final case class CompoundKey(position: String) extends StaticAnnotation

}
