package com.jcdecaux.datacorp.spark.config

import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import org.scalatest.FunSuite

class ConfLoaderSuite extends FunSuite {

  test("ConfigLoader beforeAll") {
    assert(Properties.get("myValue") === "test-my-value")
    assert(Properties.get("test.myValue2") === "test-my-value-loaded")
  }

  test("Cassandra config") {
    assert(TypesafeConfigUtils.getAs[String](Properties.cassandraConfig, "storage").get === "CASSANDRA")
    assert(TypesafeConfigUtils.getAs[String](Properties.cassandraConfig, "keyspace").get === "test_space")
    assert(TypesafeConfigUtils.getAs[String](Properties.cassandraConfig, "table").get === "test_spark_connector2")
    assert(TypesafeConfigUtils.getList(Properties.cassandraConfig, "partitionKeyColumns").get === Array("partition1", "partition2"))
    assert(TypesafeConfigUtils.getList(Properties.cassandraConfig, "clusteringKeyColumns").get === Array("clustering1"))
    assert(TypesafeConfigUtils.getList(Properties.cassandraConfig, "doesntExist") === None)
  }

  test("ConfigLoader Throw exception") {

    val x = new ConfigLoader {
      override def beforeAll(): Unit = {
        System.setProperty("app.environment", "test")
        System.setProperty("myvalue", "test-my-value")
      }
    }
    assertThrows[IllegalArgumentException](x.config, "test is not a valid AppEnv value. Exception should be thrown")

    System.clearProperty("app.environment")
    System.clearProperty("myvalue")
  }

  test("ConfigLoader builder should build configloader") {

    val cl = ConfigLoader.builder()
      .setAppEnv("local")
      .setAppName("TestConfigLoaderBuilder")
      .setProperty("myJvmProperty", "myJvmPropertyValue")
      .getOrCreate()

    assert(cl.get("test.string") === "foo")
    assert(cl.get("test.variable") === "myJvmPropertyValue")
    System.clearProperty("app.environment")
    System.clearProperty("myJvmProperty")
  }

}
