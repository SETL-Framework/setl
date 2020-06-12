package com.jcdecaux.setl.storage

import java.io.{IOException, OutputStream}

import com.jcdecaux.setl.annotation.InterfaceStability

/**
 * A Compressor is able to compress an input string into a byte array and vice versa.
 */
@InterfaceStability.Evolving
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

  protected def withOutputStream[T <: OutputStream](outputStream: T)(f: T => Unit): Unit = {
    try f(outputStream) finally outputStream.close()
  }

}
