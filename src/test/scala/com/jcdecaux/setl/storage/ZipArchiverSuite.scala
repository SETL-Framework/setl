package com.jcdecaux.setl.storage

import com.jcdecaux.setl.TestObject
import com.jcdecaux.setl.config.Properties
import com.jcdecaux.setl.enums.Storage
import com.jcdecaux.setl.exception.InvalidConnectorException
import com.jcdecaux.setl.storage.connector.FileConnector
import com.jcdecaux.setl.util.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class ZipArchiverSuite extends AnyFunSuite {

  val compressor = new ZipArchiver()

  test("ZipArchiver should be able to compress a string to a Byte[]") {
    println(s"String1: ${str.getBytes().length} -> ${compressor.compress(str).length}")
    println(s"String2: ${str2.getBytes().length} -> ${compressor.compress(str2).length}")
    println(s"String3: ${str3.getBytes().length} -> ${compressor.compress(str3).length}")
    println(s"String4: ${str4.getBytes().length} -> ${compressor.compress(str4).length}")

    assert(str.getBytes().length >= compressor.compress(str).length)
    assert(str2.getBytes().length >= compressor.compress(str2).length)
    assert(str3.getBytes().length >= compressor.compress(str3).length)
    assert(str4.getBytes().length >= compressor.compress(str4).length)
    assert(compressor.compress("") === null)
  }

  test("ZipArchiver should be able to decompress a Byte array to string") {
    assert(compressor.decompress(compressor.compress(str)) === str)
    assert(compressor.decompress(compressor.compress(str2)) === str2)
    assert(compressor.decompress(compressor.compress(str3)) === str3)
    assert(compressor.decompress(compressor.compress(str4)) === str4)
    assert(compressor.decompress(Array()) === "")
  }

  test("Archive") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val connector: FileConnector = new FileConnector(Map[String, String](
      "path" -> "src/test/resources/test-archiver/test-input",
      "inferSchema" -> "true",
      "header" -> "true",
      "saveMode" -> "Overwrite",
      "storage" -> "CSV"
    )) {
      override val storage: Storage = Storage.CSV
    }
    assertThrows[NoSuchElementException](compressor.archive(new Path(connector.basePath.getParent, "output.zip")))

    IOUtils.withTempDir { dir =>
      compressor
        .addConnector(connector, Some("dir"))
        .addFile(new Path("src/test/resources/test-archiver/test-input-file.txt"), Some("my_file.txt"))
        .archive(new Path(dir.getCanonicalPath, "output.zip"))
    }
  }

  test("ZipArchiver should throw exception") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    IOUtils.withTempDir { dir =>
      val fileRepo = new SparkRepositoryBuilder[TestObject](Storage.PARQUET)
        .setTable("test")
        .setPath(dir.getCanonicalPath)
        .getOrCreate()
      val dbRepo = new SparkRepositoryBuilder[TestObject](Properties.cassandraConfig).getOrCreate()
      val cp = new ZipArchiver()
      cp.addRepository(fileRepo)
      assertThrows[InvalidConnectorException](cp.addRepository(dbRepo))
    }
  }

  test("Archiver with local file") {
    val archiver = new ZipArchiver
    IOUtils.withTempDir { dir =>
      archiver
        .addFile(new Path("src/test/resources/test-archiver/test-input-file.txt"))
        .archive(new Path(dir.getCanonicalPath, "test-input-file.zip"))
    }

  }
}
