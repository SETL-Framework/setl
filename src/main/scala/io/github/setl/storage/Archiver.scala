package io.github.setl.storage

import io.github.setl.exception.InvalidConnectorException
import io.github.setl.storage.connector.FileConnector
import io.github.setl.storage.repository.SparkRepository
import org.apache.hadoop.fs.Path

trait Archiver {

  def addFile(file: Path, name: Option[String] = None): this.type

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
  def addRepository(repository: SparkRepository[_], name: Option[String] = None): this.type

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
  def addConnector(connector: FileConnector, name: Option[String] = None): this.type

  def archive(outputPath: Path): this.type

}
