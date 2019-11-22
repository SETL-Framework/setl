package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.Converter
import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.spark.sql.Dataset

/**
  * DatasetConverter inherits from a Converter. It can convert between two Dataset: Dataset[A] and Dataset[B]
  *
  * @tparam A Type of Dataset[A]
  * @tparam B Type of Dataset[B]
  */
@InterfaceStability.Evolving
abstract class DatasetConverter[A, B] extends Converter {

  override type T1 = Dataset[A]
  override type T2 = Dataset[B]

}
