package com.jcdecaux.setl.transformation

trait AbstractFactory[A] {

  def read(): this.type

  def process(): this.type

  def write(): this.type

  def get(): A

}
