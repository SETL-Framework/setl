package com.jcdecaux.datacorp.spark.config

import com.jcdecaux.datacorp.spark.util.TypesafeConfigUtils
import org.scalatest.FunSuite

class ConfLoaderSuite extends FunSuite {

  test("Cassandra config") {
    assert(TypesafeConfigUtils.getAs[String](Properties.cassandraConfig, "storage").get === "CASSANDRA")
    assert(TypesafeConfigUtils.getAs[String](Properties.cassandraConfig, "keyspace").get === "test_space")
    assert(TypesafeConfigUtils.getAs[String](Properties.cassandraConfig, "table").get === "test_spark_connector2")
    assert(TypesafeConfigUtils.getList(Properties.cassandraConfig, "partitionKeyColumns").get === Array("partition1", "partition2"))
    assert(TypesafeConfigUtils.getList(Properties.cassandraConfig, "clusteringKeyColumns").get === Array("clustering1"))
    assert(TypesafeConfigUtils.getList(Properties.cassandraConfig, "doesntExist") === None)
  }

}
