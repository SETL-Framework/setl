package com.jcdecaux.setl.internal

/**
 * Indicate that users can activate or deactivate the write of the class
 */
trait Writable {

  protected var _write: Boolean = true

  /**
   * Whether invoke the write method or not
   * @param write if set to true, then the write method of the factory will be invoked
   * @return
   */
  def writable(write: Boolean): this.type = {
    this._write = write
    this
  }

  /** Return true if the write method will be invoked by the pipeline */
  def writable: Boolean = this._write

}
