package com.jcdecaux.setl.storage.connector

import com.jcdecaux.setl.annotation.{Experimental, InterfaceStability}
import com.jcdecaux.setl.config.{Conf, StructuredStreamingConnectorConf}
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.util.TypesafeConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * :: Experimental ::
 *
 * Spark Structured Streaming connector
 *
 * @param conf configuration, see <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html">
 *             Spark structured streaming documentation</a> for details
 */
@Experimental
@InterfaceStability.Unstable
class StructuredStreamingConnector(val conf: StructuredStreamingConnectorConf) extends StreamingConnector {

  private[this] var streamingQuery: StreamingQuery = _

  def this(options: Map[String, String]) = this(StructuredStreamingConnectorConf.fromMap(options))

  def this(config: Config) = this(TypesafeConfigUtils.getMap(config))

  def this(config: Conf) = this(config.toMap)

  override val storage: Storage = Storage.STRUCTURED_STREAMING

  @inline protected val streamReader: DataStreamReader = spark.readStream
    .format(conf.getFormat)
    .options(conf.getReaderConf)

  protected val streamWriter: DataFrame => DataStreamWriter[Row] = (df: DataFrame) => {
    df.writeStream
      .outputMode(conf.getOutputMode)
      .format(conf.getFormat)
      .options(conf.getWriterConf)
  }

  override def read(): DataFrame = {
    if (conf.has(StructuredStreamingConnectorConf.SCHEMA)) {
      logInfo("Apply user-defined schema")
      streamReader
        .schema(conf.getSchema)
        .load()
    } else {
      streamReader.load()
    }
  }

  override def write(t: DataFrame, suffix: Option[String]): Unit = {
    logWarning("Suffix will be ignored by StructuredStreamingConnector")
    write(t)
  }

  override def write(t: DataFrame): Unit = {
    streamingQuery = streamWriter(t).start()
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   */
  override def awaitTermination(): Unit = streamingQuery.awaitTermination()

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   *
   * @param timeout time to wait in milliseconds
   * @return `true` if it's stopped; or throw the reported error during the execution; or `false`
   *         if the waiting time elapsed before returning from the method.
   */
  override def awaitTerminationOrTimeout(timeout: Long): Boolean = streamingQuery.awaitTermination(timeout)

  /**
   * Stops the execution of this query if it is running.
   */
  override def stop(): Unit = streamingQuery.stop()
}
