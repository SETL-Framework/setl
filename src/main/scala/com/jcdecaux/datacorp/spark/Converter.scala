package com.jcdecaux.datacorp.spark

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability

/**
 * A converter should be able to convert between two types T1 and T2.
 */
@InterfaceStability.Evolving
trait Converter {
  type T1
  type T2

  /**
   * Convert from an object of type T2 to an object of type T1
   *
   * @param t2 object of type T2
   * @return an object of type T1
   */
  def convertFrom(t2: T2): T1

  /**
   * Convert an object of type T1 to an object of type T2
   *
   * @param t1 object of type T1 to be convert to T2
   * @return an object of type T2
   */
  def convertTo(t1: T1): T2
}
