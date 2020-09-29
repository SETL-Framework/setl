package com.jcdecaux.setl.util

import java.io.{File, IOException}
import java.util.UUID

import com.jcdecaux.setl.internal.Logging

object IOUtils extends Logging {

  def createDirectory(root: String, namePrefix: String = "setl"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }

    dir.getCanonicalFile
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir"), namePrefix: String = "setl"): File = {
    val dir = createDirectory(root, namePrefix)
    dir
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  def withTempDir(f: File => Unit): Unit = {
    val dir = createTempDir().getCanonicalFile
    try f(dir) finally {
      deleteRecursively(dir)
    }
  }

  def deleteRecursively(file: File): Unit = {
    logDebug(s"Remove ${file.getCanonicalPath}")
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}
