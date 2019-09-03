package com.jcdecaux.datacorp.spark.storage.connector

import java.net.{URI, URLDecoder, URLEncoder}

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.ConnectorConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

@InterfaceStability.Evolving
abstract class FileConnector(val spark: SparkSession,
                             val options: ConnectorConf) extends Connector {

  // Note: this constructor was added to keep the backward compatibility
  def this(spark: SparkSession, options: Map[String, String]) = this(spark, ConnectorConf.fromMap(options))

  private[this] val hadoopConfiguration: Configuration = spark.sparkContext.hadoopConfiguration

  val schema: Option[StructType] = options.getSchema match {
    case Some(sm) =>
      log.debug("Detect user-defined schema")
      Option(StructType.fromDDL(sm))
    case _ => None
  }

  private[this] val _recursive: Boolean = true

  /**
    * Partition columns when writing the data frame
    */
  private[connector] val partition: ArrayBuffer[String] = ArrayBuffer()

  private[connector] var withSuffix: Option[Boolean] = None

  private[connector] var userDefinedSuffix: String = "_user_defined_suffix"

  private[connector] var dropUserDefinedSuffix: Boolean = true

  /**
    * Create a URI of the given file path.
    * If there are special characters in the string of path (like whitespace), then we
    * try to firstly encode the string and then create a URI
    */
  private[connector] val pathURI: URI = try {
    URI.create(options.getPath)
  } catch {
    case _: IllegalArgumentException =>
      log.warn("Can't create URI from path. Try encoding it")
      URI.create(URLEncoder.encode(options.getPath, options.getEncoding))
    case e: Throwable => throw e
  }

  /**
    * Get the current filesystem based on the path URI
    */
  private[connector] val fileSystem: FileSystem = {
    options.getS3CredentialsProvider match {
      case Some(v) => hadoopConfiguration.set("fs.s3a.aws.credentials.provider", v)
      case _ =>
    }

    options.getS3AccessKey match {
      case Some(v) => hadoopConfiguration.set("fs.s3a.access.key", v)
      case _ =>
    }

    options.getS3SecretKey match {
      case Some(v) => hadoopConfiguration.set("fs.s3a.secret.key", v)
      case _ =>
    }

    options.getS3SessionToken match {
      case Some(v) => hadoopConfiguration.set("fs.s3a.session.token", v)
      case _ =>
    }

    FileSystem.get(pathURI, hadoopConfiguration)
  }

  /**
    * Absolute path of the given path string according to the current filesystem.
    * If the filesystem is a local system, then we try to decode the path string to remove encoded
    * characters like whitespace "%20%", etc
    */
  private[connector] val absolutePath: Path = if (fileSystem.isInstanceOf[LocalFileSystem]) {
    log.debug(s"Detect local file system: ${pathURI.toString}")
    new Path(URLDecoder.decode(pathURI.toString, options.getEncoding))
  } else {
    log.debug(s"Detect distributed filesystem: ${pathURI.toString}")
    new Path(pathURI)
  }

  /**
    * Get the basePath of the current path. If the value path is a file path, then its basePath will be
    * it's parent's path. Otherwise it will be the current path itself.
    */
  private[connector] val baseDirectory: String = {
    if (fileSystem.exists(absolutePath)) {
      if (fileSystem.getFileStatus(absolutePath).isDirectory) {
        absolutePath.toString
      } else {
        absolutePath.getParent.toString
      }
    } else {
      absolutePath.getParent.toString
    }
  }

  override val reader: DataFrameReader = schema match {
    case Some(sm) => initReader().schema(sm)
    case _ => initReader()
  }

  override var writer: DataFrameWriter[Row] = _

  private[connector] val filenamePattern: Option[Regex] = options.getFilenamePattern match {
    case Some(pattern) =>
      log.debug("Detect filename pattern")
      Some(pattern.r)
    case _ => None
  }

  private[connector] def listFiles(): Array[String] = {
    listPaths().map(_.toString)
  }

  private[connector] def listPaths(): Array[Path] = {
    val filePaths = ArrayBuffer[Path]()
    val files = fileSystem.listFiles(absolutePath, true)

    while (files.hasNext) {
      val file = files.next()

      filenamePattern match {
        case Some(regex) =>
          // If the regex pattern is defined
          file.getPath.getName match {
            case regex(_*) => filePaths += file.getPath
            case _ =>
          }
        case _ =>
          // if regex not defined, append file path to the output
          filePaths += file.getPath
      }
    }
    log.debug(s"Find ${filePaths.length} files")
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

  @inline private[connector] def initReader(): DataFrameReader = {
    this.spark.read.option("basePath", baseDirectory).options(options.getDataFrameReaderOptions)
  }

  /**
    * Initialize a DataFrame writer. A new writer will be initiate only if the hashcode
    * of input DataFrame is different than the last written DataFrame.
    *
    * @param df input DataFrame
    */
  @inline private[connector] def initWriter(df: DataFrame): Unit = {

    // Check if the writer is already initialized by comparing hashcode
    if (df.hashCode() != lastWriteHashCode) {
      // Save the current DataFrame's hashcode
      lastWriteHashCode = df.hashCode()

      val _df = schema match {
        case Some(sm) => // If schema is defined, reorder df's columns
          log.debug("Detect schema, reorder columns before writing")
          val cols = sm.map(_.name)
          df.select(cols.map(functions.col): _*)

        case _ => df // Otherwise, do nothing
      }

      writer = _df.write
        .mode(options.getSaveMode)
        .options(options.getDataFrameWriterOptions)
        .partitionBy(partition: _*)

    }
  }

  def partitionBy(columns: String*): this.type = {
    log.debug(s"Data will be partitioned by ${columns.mkString(", ")}")
    partition.append(columns: _*)
    this
  }

  /**
    * Delete the current file or directory
    */
  def delete(): Unit = {
    log.debug(s"Delete $absolutePath")
    fileSystem.delete(absolutePath, _recursive)
    withSuffix = None
  }

  /**
    * Get the sum of file size
    *
    * @return size in byte
    */
  def getSize: Long = {
    listPaths().map(path => fileSystem.getFileStatus(path).getLen).sum
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  def _write(df: DataFrame, filepath: String): Unit = {
    log.debug(s"Write DataFrame to $filepath")
    initWriter(df)
    writer.format(options.getStorage.toString.toLowerCase()).save(filepath)
  }

  /**
    * Write a [[DataFrame]] into file
    *
    * @param df     dataframe to be written
    * @param suffix optional, String, write the df in a sub-directory of the defined path
    */
  override def write(df: DataFrame, suffix: Option[String] = None): Unit = {
    suffix match {
      case Some(s) =>
        checkPartitionValidity(true)
        _write(df, s"${this.absolutePath.toString}/$userDefinedSuffix=$s")
      case _ =>
        checkPartitionValidity(false)
        _write(df, this.absolutePath.toString)
    }
  }

  /**
    * Read a [[DataFrame]] from a file with the path defined during the instantiation.
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading ${options.getStorage.toString} file from ${absolutePath.toString}")

    val df = reader.format(options.getStorage.toString.toLowerCase()).load(listFiles(): _*)

    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffix)) {
      df.drop(userDefinedSuffix)
    } else {
      df
    }
  }
}
