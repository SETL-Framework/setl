package io.github.setl.config

import io.github.setl.enums.Storage
import io.github.setl.exception.ConfException
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class FileConnectorConfSuite extends AnyFunSuite {

  val conf = new FileConnectorConf()


  test("Set FileConnectorConf") {
    assert(conf.get("storage") === None)
    conf.setStorage("CSV")
    assert(conf.get("storage").get === "CSV")
    conf.setStorage(Storage.EXCEL)
    assert(conf.get("storage").get === "EXCEL")

    assert(conf.get("encoding") === None)
    conf.setEncoding("latin-1")
    assert(conf.get("encoding").get === "latin-1")

    assert(conf.get("saveMode") === None)
    conf.setSaveMode("Append")
    assert(conf.get("saveMode").get === "Append")
    conf.setSaveMode(SaveMode.Overwrite)
    assert(conf.get("saveMode").get === "Overwrite")

    assert(conf.get("path") === None)
    conf.setPath("path")
    assert(conf.get("path").get === "path")

    assert(conf.get("credentialsProvider") === None)
    conf.setS3CredentialsProvider("credentialsProvider")
    assert(conf.get("fs.s3a.aws.credentials.provider").get === "credentialsProvider")

    assert(conf.get("accessKey") === None)
    conf.setS3AccessKey("accessKey")
    assert(conf.get("fs.s3a.access.key").get === "accessKey")

    assert(conf.get("secretKey") === None)
    conf.setS3SecretKey("secretKey")
    assert(conf.get("fs.s3a.secret.key").get === "secretKey")

    assert(conf.get("sessionToken") === None)
    conf.setS3SessionToken("sessionToken")
    assert(conf.get("fs.s3a.session.token").get === "sessionToken")
  }

  test("Getters FileConnectorConf") {
    assert(conf.getEncoding === "latin-1")
    assert(conf.getSaveMode === SaveMode.Overwrite)
    assert(conf.getStorage === Storage.EXCEL)
    assert(conf.getPath === "path")
    assert(conf.getSchema === None)
    assert(conf.getS3CredentialsProvider === Some("credentialsProvider"))
    assert(conf.getS3AccessKey === Some("accessKey"))
    assert(conf.getS3SecretKey === Some("secretKey"))
    assert(conf.getS3SessionToken === Some("sessionToken"))
    assert(conf.getFilenamePattern === None)

    val newConf = new FileConnectorConf()
    assertThrows[ConfException](newConf.getStorage)
    assertThrows[ConfException](newConf.getPath)
  }
}
