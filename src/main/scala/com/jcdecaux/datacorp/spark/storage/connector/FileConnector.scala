package com.jcdecaux.datacorp.spark.storage.connector

import java.net.{URI, URLDecoder, URLEncoder}

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Evolving
trait FileConnector extends Connector {

  val path: String
  val saveMode: SaveMode

  private[this] val encoding: String = "UTF-8"

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

  private[this] val fileSystem: FileSystem = FileSystem.get(pathURI, new Configuration())

  private[connector] val absolutePath: Path = if (fileSystem.isInstanceOf[LocalFileSystem]) {
    log.debug("Find local filesystem")
    new Path(URLDecoder.decode(pathURI.toString, encoding))
  } else {
    log.debug(s"Find ${fileSystem.getClass.toString}")
    new Path(pathURI)
  }

  private[connector] val basePath: String = if (fileSystem.isDirectory(absolutePath)) {
    absolutePath.toString
  } else {
    absolutePath.getParent.toString
  }

  private[connector] val partition: ArrayBuffer[String] = ArrayBuffer()

  def partitionBy(columns: String*): this.type = {
    partition.append(columns: _*)
    this
  }

  def delete(): Unit = {
    fileSystem.delete(absolutePath, _recursive)
    withSuffix = None
  }

  def listFiles(): Array[String] = {
    val filePaths = ArrayBuffer[String]()
    val files = fileSystem.listFiles(absolutePath, true)

    while (files.hasNext) {
      val file = files.next()
      filePaths += file.getPath.toString
    }
    filePaths.toArray
  }

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
