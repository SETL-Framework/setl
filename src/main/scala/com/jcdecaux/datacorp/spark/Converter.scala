package com.jcdecaux.datacorp.spark

trait Converter {
  type T1
  type T2

  def convertFrom(t2: T2): T1

  def convertTo(t1: T1): T2
}
