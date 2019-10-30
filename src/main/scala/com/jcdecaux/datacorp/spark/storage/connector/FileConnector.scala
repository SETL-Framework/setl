package com.jcdecaux.datacorp.spark.storage.connector

import java.net.{URI, URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import com.jcdecaux.datacorp.spark.annotation.InterfaceStability
import com.jcdecaux.datacorp.spark.config.ConnectorConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.annotation.tailrec
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

  private[this] val writeCount: AtomicLong = new AtomicLong(0L)

  /**
    * 0: suffix not initialized yet
    * 1: with suffix
    * 2: without suffix
    */
  private[this] val suffixState: AtomicInteger = new AtomicInteger(0)

  /**
    * Lock that will be used when configuring suffix
    */
  private[this] val lock: ReentrantLock = new ReentrantLock()

  /**
    * FileConnector will create a sub-directory for a given suffix. The name of directory respects the naming convention
    * of HIVE partition:
    *
    * {{{
    *   ../_user_defined_suffix=xxx
    * }}}
    */
  private[this] var UDSKey: String = "_user_defined_suffix"

  // use a ThreadLocal object to keep it thread safe
  private[this] val UDSValue: ThreadLocal[Option[String]] = new ThreadLocal[Option[String]]() {
    override def initialValue(): Option[String] = None
  }

  private[this] var _dropUDS: Boolean = true

  /**
    * Set to true to drop the column containing user defined suffix (default name _user_defined_suffix)
    *
    * @param boo true to drop, false to keep
    */
  def dropUserDefinedSuffix(boo: Boolean): this.type = {
    _dropUDS = boo
    this
  }

  /**
    * Get the boolean value of dropUserDefinedSuffix.
    *
    * @return true if the column will be dropped, false otherwise
    */
  def dropUserDefinedSuffix: Boolean = _dropUDS

  /**
    * Set the name of user defined suffix column (by default is _user_defined_suffix
    *
    * @param key name of the new key
    */
  def setUserDefinedSuffixKey(key: String): this.type = {
    UDSKey = key
    this
  }

  /**
    * Get the value of user defined suffix column name
    *
    * @return
    */
  def getUserDefinedSuffixKey: String = this.UDSKey

  def getWriteCount: Long = this.writeCount.get()


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
    * Get the current filesystem based on the path URI
    */
  def getFileSystem: FileSystem = this.fileSystem

  /**
    * Absolute path of the given path string according to the current filesystem.
    * If the filesystem is a local system, then we try to decode the path string to remove encoded
    * characters like whitespace "%20%", etc
    */
  private[connector] val absolutePath: Path = {
    log.debug(s"File system URI: ${fileSystem.getUri}")

    if (fileSystem.isInstanceOf[LocalFileSystem]) {
      new Path(URLDecoder.decode(pathURI.toString, options.getEncoding))
    } else {
      new Path(pathURI)
    }
  }

  /**
    * Get the basePath of the current path. If the value path is a file path, then its basePath will be
    * it's parent's path. Otherwise it will be the current path itself.
    */
  @throws[java.io.FileNotFoundException](s"$absolutePath doesn't exist")
  def basePath: Path = {
    val bp = findBasePath(absolutePath)

    if (fileSystem.getFileStatus(absolutePath).isDirectory) {
      bp
    } else {
      bp.getParent
    }
  }

  @tailrec
  private[this] def findBasePath(path: Path): Path = {
    if (path.getName.split("=").length == 2) {
      findBasePath(path.getParent)
    } else {
      path
    }
  }

  /**
    * DataFrame reader for the current path of connector
    */
  override lazy val reader: DataFrameReader = schema match {
    case Some(sm) => initReader().schema(sm)
    case _ => initReader()
  }

  private[this] val filenamePattern: Option[Regex] = options.getFilenamePattern match {
    case Some(pattern) =>
      log.debug("Detect filename pattern")
      Some(pattern.r)
    case _ => None
  }

  /**
    * List all the file path (in format of string) to be loaded.
    *
    * <p>If the current connector has a non-empty filename pattern, then return a list of file paths that match
    * the pattern.</p>
    *
    * <p>When the filename pattern is not set: If the absolute path of this connector is a directory, return the path of the directory if detailed is set
    * to false. Otherwise, return a list of file paths in the directory</p>
    *
    * @param detailed
    * @return
    */
  def listFilesToLoad(detailed: Boolean = true): Array[String] = filesToLoad(detailed).map(_.toString)

  /**
    * List ALL the file paths (in format of string) of the current path of connector
    *
    * @return
    */
  def listFiles(): Array[String] = listPaths().map(_.toString)

  /**
    * List ALL the file paths of the current path of connector
    *
    * @return
    */
  def listPaths(): Array[Path] = {
    val filePaths = ArrayBuffer[Path]()
    val files = fileSystem.listLocatedStatus(absolutePath)

    while (files.hasNext) {
      val status = files.next()
      if (status.isFile) {
        filePaths += status.getPath
      }
    }
    log.debug(s"Find ${filePaths.length} files")
    filePaths.toArray
  }

  /**
    * List files to be loaded.
    * <p>If the current connector has a non-empty filename pattern, then return a list of file paths that match
    * the pattern.</p>
    *
    * <p>When the filename pattern is not set: If the absolute path of this connector is a directory, return the path of the directory if detailed is set
    * to false. Otherwise, return a list of file paths in the directory</p>
    *
    * @param detailed true to return a list of file paths if the current absolute path is a directory
    * @return
    */
  def filesToLoad(detailed: Boolean): Array[Path] = {
    filenamePattern match {
      case Some(pattern) =>
        listPaths().filter(p => p.getName.matches(pattern.toString()))
      case _ =>
        if (detailed) {
          listPaths()
        } else {
          Array(absolutePath)
        }
    }
  }

  /**
    * <p>Validate the given suffix.</p>
    * <p>In the case where data were written without suffix, an [[IllegalArgumentException]] will be thrown when one tries
    * to write new data with suffix.</p>
    * <p>In the case where data were written with suffix, a new `null` suffix will be transformed into the default suffix
    * value `default`</p>
    *
    * @param suffix suffix to be validated
    * @return a validated suffix
    */
  @throws[IllegalArgumentException]
  @throws[RuntimeException]
  private[this] def validateSuffix(suffix: Option[String]): Option[String] = {

    if (suffixState.get() == 1) {
      // If suffix is set
      suffix match {
        case Some(_) => suffix
        case _ =>
          log.info("Can't remove user defined suffix (UDS) when another " +
            "UDS has already been saved. Replace it with 'default'")
          Some("default")
      }

    } else if (suffixState.get() == 2) {
      // If there is no suffix
      suffix match {
        case Some(s) =>
          throw new IllegalArgumentException(s"Can't set suffix ${s}. " +
            s"Current version of ${this.getClass.getSimpleName} " +
            s"doesn't support adding an user defined suffix into already-saved non-suffix data")

        case _ => suffix
      }

    } else {
      throw new RuntimeException(s"Wrong suffix lock value: ${suffixState.get()}")
    }
  }

  private[this] def updateSuffixState(suffix: Option[String]): Unit = {
    log.debug(s"(${Thread.currentThread().getId}) Update suffix state.")
    suffix match {
      case Some(_) => suffixState.set(1) // with suffix
      case None => suffixState.set(2) // without suffix
    }
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

    val locked = lock.tryLock(10, TimeUnit.SECONDS)

    val _suffix = if (locked) {
      // log.debug(s"(${Thread.currentThread().getId}) Acquire lock")
      try {
        if (suffixState.get() == 0) updateSuffixState(suffix)
        validateSuffix(suffix)
      } finally {
        // log.debug(s"(${Thread.currentThread().getId}) Release lock")
        lock.unlock()
      }
    } else {
      throw new RuntimeException(s"(${Thread.currentThread().getId}) Can't acquire lock")
    }
    this.UDSValue.set(_suffix)

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
      this.UDSValue.set(None)
      this.writeCount.set(0L)
      this.suffixState.set(0)
    } else {
      setSuffix(None)
    }
    this
  }

  @inline private[this] def initReader(): DataFrameReader = {
    this.spark.read.options(options.getDataFrameReaderOptions)
  }

  /**
    * Initialize a DataFrame writer. A new writer will be initiate only if the hashcode
    * of input DataFrame is different than the last written DataFrame.
    */
  @inline override val writer: DataFrame => DataFrameWriter[Row] = (df: DataFrame) => {
    val reOrderedDf = schema match {
      case Some(sm) => // If schema is defined, reorder df's columns
        log.debug("Detect schema, reorder columns before writing")
        val schemaColumns = sm.map(_.name)
        df.select(schemaColumns.map(functions.col): _*)

      case _ => df // Otherwise, do nothing
    }

    reOrderedDf.write
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
    resetSuffix(true)
  }

  /**
    * Get the sum of file size
    *
    * @return size in byte
    */
  def getSize: Long = {
    filesToLoad(true).map(path => fileSystem.getFileStatus(path).getLen).sum
  }

  /**
    * Write a [[DataFrame]] into the given path with the given save mode
    */
  private[connector] def writeToPath(df: DataFrame, filepath: String): Unit = {
    log.debug(s"(${Thread.currentThread().getId}) Write DataFrame to $filepath")
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

  override def write(t: DataFrame): Unit = writeToPath(t, outputPath)

  private[connector] def outputPath: String = UDSValue.get() match {
    case Some(suffix) => s"${this.absolutePath.toString}/$UDSKey=$suffix"
    case _ => this.absolutePath.toString
  }

  /**
    * Read a [[DataFrame]] from a file with the path defined during the instantiation.
    *
    * @return
    */
  @throws[java.io.FileNotFoundException](s"$absolutePath doesn't exist")
  override def read(): DataFrame = {
    log.debug(s"Reading ${options.getStorage.toString} file in: '${absolutePath.toString}'")

    val df = reader
      .option("basePath", basePath.toString)
      .format(options.getStorage.toString.toLowerCase())
      .load(listFilesToLoad(false): _*)

    if (dropUserDefinedSuffix & df.columns.contains(UDSKey)) {
      df.drop(UDSKey)
    } else {
      df
    }
  }
}
