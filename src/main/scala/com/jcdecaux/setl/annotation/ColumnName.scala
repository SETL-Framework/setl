package com.jcdecaux.setl.annotation

import scala.annotation.StaticAnnotation

/**
 * Define an alias for the current field in the table
 *
 * @param name alias of the current field name
 */
@InterfaceStability.Stable
final case class ColumnName(name: String) extends StaticAnnotation
