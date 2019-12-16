package com.jcdecaux.setl.storage

import org.scalatest.funsuite.AnyFunSuite

class XZCompressorSuite extends AnyFunSuite {

  val compressor = new XZCompressor

  test("XZCompressor should be able to compress a string to a Byte[]") {
    println(s"String1: ${str.getBytes().length} -> ${compressor.compress(str).length}")
    println(s"String2: ${str2.getBytes().length} -> ${compressor.compress(str2).length}")
    println(s"String3: ${str3.getBytes().length} -> ${compressor.compress(str3).length}")
    println(s"String4: ${str4.getBytes().length} -> ${compressor.compress(str4).length}")

    assert(str.getBytes().length >= compressor.compress(str).length)
    assert(str2.getBytes().length >= compressor.compress(str2).length)
    assert(str3.getBytes().length >= compressor.compress(str3).length)
    assert(str4.getBytes().length >= compressor.compress(str4).length)

  }

  test("XZCompressor should be able to decompress a Byte array to string") {
    assert(compressor.decompress(compressor.compress(str)) === str)
    assert(compressor.decompress(compressor.compress(str2)) === str2)
    assert(compressor.decompress(compressor.compress(str3)) === str3)
    assert(compressor.decompress(compressor.compress(str4)) === str4)
  }

}
