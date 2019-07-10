package com.jcdecaux.datacorp.spark.storage.connector

import java.net.URI

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ArrayBuffer

@InterfaceStability.Evolving
trait FileConnector extends Connector {

  val path: String
  val saveMode: SaveMode

  private[connector] var withSuffix: Option[Boolean] = None

  private[connector] def checkPartitionValidity(suffix: Boolean): Unit = {
    if (_partition.nonEmpty) {
      withSuffix match {
        case Some(boo) =>
          if (boo != suffix)
            throw new IllegalArgumentException("Current version doesn't support mixing " +
              "suffix with non-suffix when the data table is partitioned")
        case _ => withSuffix = Some(suffix)
      }
    }
  }

  private[connector] var userDefinedSuffix: String = "_user_defined_suffix"
  private[connector] var dropUserDefinedSuffix: Boolean = true

  private[connector] val _path: Path = new Path(path)
  private[connector] var _recursive: Boolean = true
  private[connector] val _fs: FileSystem = FileSystem.get(URI.create(path), new Configuration())
  private[connector] val _partition: ArrayBuffer[String] = ArrayBuffer()

  def partitionBy(columns: String*): this.type = {
    _partition.append(columns: _*)
    this
  }

  def delete(): Unit = {
    _fs.delete(_path, _recursive)
    withSuffix = None
  }

  def listFiles(): Array[String] = {
    val filePaths = ArrayBuffer[String]()
    val files = _fs.listFiles(_path, true)

    while (files.hasNext) {
      val file = files.next()
      filePaths += file.getPath.toString
    }
    filePaths.toArray
  }
}
