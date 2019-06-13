package com.jcdecaux.datacorp.spark

import scala.annotation.StaticAnnotation

package object annotations {

  final case class colName(name: String) extends StaticAnnotation

}
