package com.jcdecaux.datacorp.spark.storage.connector

import java.net.{URI, URLDecoder, URLEncoder}
import java.util.concurrent.atomic.AtomicLong

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
  private[this] val partition: ArrayBuffer[String] = ArrayBuffer()

  private[connector] var writeCount: AtomicLong = new AtomicLong(0L)

  private[connector] var userDefinedSuffixKey: String = "_user_defined_suffix"

  // use a ThreadLocal object to keep it thread safe
  private[this] val userDefinedSuffixValue: ThreadLocal[Option[String]] = new ThreadLocal[Option[String]]() {
    override def initialValue(): Option[String] = None
  }

  private[connector] var dropUserDefinedSuffix: Boolean = true

  /**
    * Create a URI of the given file path.
    * If there are special characters in the string of path (like whitespace), then we
    * try to firstly encode the string and then create a URI
    */
  private[this] val pathURI: URI = try {
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
  private[this] val fileSystem: FileSystem = {
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
  private[this] val absolutePath: Path = if (fileSystem.isInstanceOf[LocalFileSystem]) {
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
  private[this] val baseDirectory: String = {
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

  private[this] val filenamePattern: Option[Regex] = options.getFilenamePattern match {
    case Some(pattern) =>
      log.debug("Detect filename pattern")
      Some(pattern.r)
    case _ => None
  }

  private[connector] def listFiles(): Array[String] = {
    listPaths().map(_.toString)
  }

  private[this] def listPaths(): Array[Path] = {
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
    * @param suffix an option of suffix in string format
    */
  def setSuffix(suffix: Option[String]): this.type = {

    //    val _suffix =
    //
    //      if (writeCount.get() == 0) {
    //        suffix
    //      } else {
    //        if (this.userDefinedSuffixValue.get().isDefined) {
    //
    //          suffix match {
    //            case Some(_) => suffix
    //            case _ =>
    //              log.info("Can't remove user defined suffix (UDS) when another " +
    //                "UDS has already been saved. Replace it with 'default'")
    //              Some("default")
    //          }
    //
    //        } else {
    //
    //          suffix match {
    //            case Some(s) =>
    //              throw new IllegalArgumentException(s"Can't set suffix ${s}. " +
    //                s"Current version of ${this.getClass.getSimpleName} " +
    //                s"doesn't support adding an user defined suffix into already-saved non-suffix data")
    //
    //            case _ =>
    //              log.debug("Set suffix None to non-suffix data")
    //              suffix
    //          }
    //        }
    //      }

    this.userDefinedSuffixValue.set(suffix)
    this
  }

  /**
    * Reset suffix to None
    *
    * @param force set to true to ignore the validity check of suffix value
    */
  def resetSuffix(force: Boolean = false): this.type = {
    if (force) {
      log.warn("Reset suffix may cause unexpected behavior of FileConnector")
      this.userDefinedSuffixValue.set(None)
      this.writeCount.set(0L)
    } else {
      setSuffix(None)
    }
    this
  }

  @inline private[this] def initReader(): DataFrameReader = {
    this.spark.read.option("basePath", baseDirectory).options(options.getDataFrameReaderOptions)
  }

  /**
    * Initialize a DataFrame writer. A new writer will be initiate only if the hashcode
    * of input DataFrame is different than the last written DataFrame.
    */
  @inline override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    val _df = schema match {
      case Some(sm) => // If schema is defined, reorder df's columns
        log.debug("Detect schema, reorder columns before writing")
        val schemaColumns = sm.map(_.name)

        //        val generatedColumns = df.columns
        //          .filterNot(schemaColumns.contains)
        //            .filter{
        //              name =>
        //                val elem = name.split(SchemaConverter.compoundKeySeparator)
        //                elem.head == SchemaConverter.compoundKeyPrefix && elem.last == SchemaConverter.compoundKeySuffix
        //            }

        df.select(schemaColumns.map(functions.col): _*)

      case _ => df // Otherwise, do nothing
    }

    _df.write
      .mode(options.getSaveMode)
      .options(options.getDataFrameWriterOptions)
      .partitionBy(partition: _*)
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
  private[this] def _write(df: DataFrame, filepath: String): Unit = {
    log.debug(s"Write DataFrame to $filepath")
    incrementWriteCounter()
    writer(df).format(storage.toString.toLowerCase()).save(filepath)

  }

  private[this] def incrementWriteCounter(): Unit = {
    if (this.writeCount.get() >= Long.MaxValue) throw new UnsupportedOperationException("Write count exceeds the max value")
    this.writeCount.getAndAdd(1L)
  }

  /**
    * Write a [[DataFrame]] into file
    *
    * @param df     dataframe to be written
    * @param suffix optional, String, write the df in a sub-directory of the defined path
    */
  override def write(df: DataFrame, suffix: Option[String]): Unit = {
    setSuffix(suffix)
    this.write(df)
  }

  override def write(t: DataFrame): Unit = _write(t, outputPath)

  private[this] def outputPath: String = userDefinedSuffixValue.get() match {
    case Some(suffix) => s"${this.absolutePath.toString}/$userDefinedSuffixKey=$suffix"
    case _ => this.absolutePath.toString
  }

  /**
    * Read a [[DataFrame]] from a file with the path defined during the instantiation.
    *
    * @return
    */
  override def read(): DataFrame = {
    log.debug(s"Reading ${options.getStorage.toString} file from ${absolutePath.toString}")

    val df = reader.format(options.getStorage.toString.toLowerCase()).load(listFiles(): _*)

    if (dropUserDefinedSuffix & df.columns.contains(userDefinedSuffixKey)) {
      df.drop(userDefinedSuffixKey)
    } else {
      df
    }
  }
}
