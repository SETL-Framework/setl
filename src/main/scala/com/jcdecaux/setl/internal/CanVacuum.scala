package com.jcdecaux.setl.internal

trait CanVacuum {

  def vacuum(retentionHours: Double): Unit
  def vacuum(): Unit

}
