package com.jcdecaux.datacorp.spark.storage

import java.io.IOException

trait Compressor extends Serializable {

  @throws[IOException]
  def compress(input: String): Array[Byte]

  @throws[IOException]
  def decompress(bytes: Array[Byte]): String

}
