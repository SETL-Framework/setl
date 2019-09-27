package com.jcdecaux.datacorp.spark.storage

import java.io.IOException

/**
  * A Compressor is able to compress an input string into a byte array and vice versa.
  */
trait Compressor extends Serializable {

  /**
    * Compress an input string into a byte array
    */
  @throws[IOException]
  def compress(input: String): Array[Byte]

  /**
    * Decompress a byte array into an input string
    */
  @throws[IOException]
  def decompress(bytes: Array[Byte]): String

}
