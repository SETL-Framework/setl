package com.jcdecaux.datacorp.spark.storage.connector

import java.net.{URI, URLDecoder, URLEncoder}

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Evolving
trait FileConnector extends Connector {

  /**
    * The path of file to be loaded.
    * It could be a file path or a directory (for CSV and Parquet).
    * In the case of a directory, the correctness of Spark partition structure should be guaranteed by user.
    */
  val path: String
  val saveMode: SaveMode

  private[this] val encoding: String = "UTF-8"

  /**
    * Create a URI of the given file path.
    * If there are special characters in the string of path (like whitespace), then we
    * try to firstly encode the string and then create a URI
    */
  private[connector] val pathURI: URI = try {
    URI.create(path)
  } catch {
    case _: IllegalArgumentException =>
      log.warn("Can't create URI from path. Try encoding it")
      URI.create(URLEncoder.encode(path, encoding))
    case e: Throwable => throw e
  }

  private[connector] var withSuffix: Option[Boolean] = None

  private[connector] var userDefinedSuffix: String = "_user_defined_suffix"

  private[connector] var dropUserDefinedSuffix: Boolean = true

  private[this] val _recursive: Boolean = true

  /**
    * Get the current filesystem based on the path URI
    */
  private[this] val fileSystem: FileSystem = FileSystem.get(pathURI, new Configuration())

  /**
    * Absolute path of the given path string according to the current filesystem.
    * If the filesystem is a local system, then we try to decode the path string to remove encoded
    * characters like whitespace "%20%", etc
    */
  private[connector] val absolutePath: Path = if (fileSystem.isInstanceOf[LocalFileSystem]) {
    log.debug("Find local filesystem")
    new Path(URLDecoder.decode(pathURI.toString, encoding))
  } else {
    log.debug(s"Find ${fileSystem.getClass.toString}")
    new Path(pathURI)
  }

  /**
    * Get the basePath of the current path. If the value path is a file path, then its basePath will be
    * it's parent's path. Otherwise it will be the current path itself.
    */
  private[connector] val basePath: String = if (fileSystem.isDirectory(absolutePath)) {
    absolutePath.toString
  } else {
    absolutePath.getParent.toString
  }

  /**
    * Partition columns when writing the data frame
    */
  private[connector] val partition: ArrayBuffer[String] = ArrayBuffer()

  def partitionBy(columns: String*): this.type = {
    partition.append(columns: _*)
    this
  }

  /**
    * Delete the current file or directory
    */
  def delete(): Unit = {
    fileSystem.delete(absolutePath, _recursive)
    withSuffix = None
  }

  private[connector] def listFiles(): Array[String] = {
    val filePaths = ArrayBuffer[String]()
    val files = fileSystem.listFiles(absolutePath, true)

    while (files.hasNext) {
      val file = files.next()
      filePaths += file.getPath.toString
    }
    filePaths.toArray
  }

  /**
    * The current version of FileConnector doesn't support a mix of suffix
    * and non-suffix write when the DataFrame is partitioned.
    *
    * This method will detect, in the case of a partitioned table, if user
    * try to use both suffix write and non-suffix write
    *
    * @param suffix boolean
    */
  private[connector] def checkPartitionValidity(suffix: Boolean): Unit = {
    if (partition.nonEmpty) {
      withSuffix match {
        case Some(boo) =>
          if (boo != suffix)
            throw new IllegalArgumentException("Current version doesn't support mixing " +
              "suffix with non-suffix when the data table is partitioned")
        case _ => withSuffix = Some(suffix)
      }
    }
  }
}
