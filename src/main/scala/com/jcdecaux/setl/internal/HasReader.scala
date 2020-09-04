package com.jcdecaux.setl.internal

import org.apache.spark.sql.DataFrameReader

trait HasReader {

  protected val reader: DataFrameReader

}
