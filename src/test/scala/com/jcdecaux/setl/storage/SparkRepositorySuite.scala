package com.jcdecaux.setl.storage

import java.io.File

object SparkRepositorySuite {
  def deleteRecursively(file: File): Unit = {
    println(s"Remove ${file.getName}")
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}