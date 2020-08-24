package com.jcdecaux.setl.internal

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

trait HasWriter {

  protected val writer: DataFrame => DataFrameWriter[Row]

}
