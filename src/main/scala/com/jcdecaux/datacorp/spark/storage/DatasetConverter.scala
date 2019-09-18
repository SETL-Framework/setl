package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.Converter
import org.apache.spark.sql.Dataset

abstract class DatasetConverter[A, B] extends Converter {

  override type T1 = Dataset[A]
  override type T2 = Dataset[B]

}
