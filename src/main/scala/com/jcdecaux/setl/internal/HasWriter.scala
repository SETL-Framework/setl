package com.jcdecaux.setl.internal

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

trait HasWriter {  Connector =>

  protected val writer: DataFrame => DataFrameWriter[Row]

}
