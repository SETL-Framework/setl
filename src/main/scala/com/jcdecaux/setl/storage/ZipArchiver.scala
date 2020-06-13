package com.jcdecaux.setl.storage

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import java.util.zip.{DeflaterOutputStream, InflaterInputStream, ZipEntry, ZipOutputStream}

import com.jcdecaux.setl.exception.InvalidConnectorException
import com.jcdecaux.setl.internal.Logging
import com.jcdecaux.setl.storage.connector.FileConnector
import com.jcdecaux.setl.storage.repository.SparkRepository
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path, UnsupportedFileSystemException}

class ZipArchiver() extends Compressor with Archiver with Logging {

  private[this] val fileEntries: ConcurrentHashMap[String, Path] = new ConcurrentHashMap[String, Path]()
  private[this] var fileSystem: Option[FileSystem] = None

  override def addFile(file: Path, name: Option[String] = None): this.type = {
    fileEntries.put(name.getOrElse(file.getName), file)
    this
  }

  /**
   * Add the connector's data to the consolidator. For a directory with the following structure:
   *
   * {{{
   *   base_path
   *      |-- dir_1
   *      |     |-- file1
   *      |-- dir_2
   *            |-- file2
   * }}}
   *
   * After calling <code>addConnector(connector, Some("new_name"))</code>, the structure in the compressed zip file will be:
   *
   * {{{
   *   outputPath.zip  // outputPath.zip is given during the instantiation of FileConsolidator
   *      |--new_name
   *           |-- dir_1
   *           |     |-- file1
   *           |-- dir_2
   *                 |-- file2
   * }}}
   *
   * @param repository Repository that will be used to load data
   * @param name       name of the directory in the zip output. default is the name of the base directory of the connector
   * @return
   */
  @throws[InvalidConnectorException]
  override def addRepository(repository: SparkRepository[_], name: Option[String] = None): this.type = {
    val fileConnector = try {
      repository.getConnector.asInstanceOf[FileConnector]
    } catch {
      case e: java.lang.ClassCastException =>
        throw new InvalidConnectorException(s"FileConsolidator only accept repositories with FileConnector. ${e.getMessage}")
    }
    addConnector(fileConnector, name)
  }

  /**
   * Add the connector's data to the consolidator. For a directory with the following structure:
   *
   * {{{
   *   base_path
   *      |-- dir_1
   *      |     |-- file1
   *      |-- dir_2
   *            |-- file2
   * }}}
   *
   * After calling <code>addConnector(connector, Some("new_name"))</code>, the structure in the compressed zip file will be:
   *
   * {{{
   *   outputPath.zip  // outputPath.zip is given during the instantiation of FileConsolidator
   *      |--new_name
   *           |-- dir_1
   *           |     |-- file1
   *           |-- dir_2
   *                 |-- file2
   * }}}
   *
   * @param connector FileConnector that will be used to load data
   * @param name      name of the directory in the zip output. default is the name of the base directory of the connector
   * @return
   */
  override def addConnector(connector: FileConnector, name: Option[String] = None): this.type = {
    if (this.fileSystem.isEmpty) {
      setFileSystem(connector.getFileSystem)
    }
    val rootPath = connector.basePath
    val rootName = name.getOrElse(rootPath.getName)
    connector.listPaths()
      .foreach { path =>
        this.addFile(path, Some(s"$rootName/${path.toString.split(rootPath.toString).last}"))
      }

    this
  }

  def setFileSystem(fs: FileSystem): this.type = {
    this.fileSystem = Some(fs)
    this
  }

  /**
   * Zip all the input entries to the given path
   *
   * @param outputPath path of the output
   * @return
   */
  @throws[UnsupportedFileSystemException]
  override def archive(outputPath: Path): this.type = {
    val fs = fileSystem.getOrElse(throw new UnsupportedFileSystemException("The file system is not set yet"))
    val hdfsDataOutputStream: FSDataOutputStream = fs.create(outputPath)

    withOutputStream(new ZipOutputStream(hdfsDataOutputStream)) {
      zipOutputStream =>
        val iterator = fileEntries.entrySet().iterator()
        while (iterator.hasNext) {
          val entrySet = iterator.next()
          val path = entrySet.getValue
          val name = entrySet.getKey
          log.debug(s"Read ${path.toString}")
          val inputStream = fs.open(path)
          val data = IOUtils.toByteArray(inputStream)
          val zipEntry = new ZipEntry(name)
          zipOutputStream.putNextEntry(zipEntry)
          zipOutputStream.write(data, 0, data.length)
          zipOutputStream.closeEntry()
        }
    }
    this
  }

  /**
   * Compress an input string into a byte array
   */
  override def compress(input: String): Array[Byte] = {
    if ((input == null) || input.isEmpty) {
      return null
    }

    val compressedBytes = new ByteArrayOutputStream()
    withOutputStream(new DeflaterOutputStream(compressedBytes)) { zipOutput =>
      zipOutput.write(input.getBytes(StandardCharsets.UTF_8))
      zipOutput.flush();
    }

    compressedBytes.toByteArray
  }

  /**
   * Decompress a byte array into an input string
   */
  override def decompress(bytes: Array[Byte]): String = {
    if ((bytes == null) || bytes.isEmpty) {
      return ""
    }

    val inputStreamReader = {
      val zipInputStream = new InflaterInputStream(new ByteArrayInputStream(bytes))
      new InputStreamReader(zipInputStream, StandardCharsets.UTF_8)
    }

    new BufferedReader(inputStreamReader).lines().collect(Collectors.joining("\n"))
  }


  /**
   * For a given output stream and a partial function f, call <code>f(outputStream)</code> and then close the stream
   *
   * @param outputStream output stream
   * @param f            function
   */
  private[this] def withOutputStream[T <: OutputStream](outputStream: T)(f: T => Unit): Unit = {
    try f(outputStream) finally outputStream.close()
  }
}
