package io.github.setl.storage

import io.github.setl.annotation.{ColumnName, CompoundKey, Compress}
import io.github.setl.internal.TestClasses.InnerClass
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder}

package object repository {

  case class RepoAdapterTesterA(col1: String, col2: String)

  case class RepoAdapterTesterB(@ColumnName("column1") col1: String, col2: String, col3: String)

  case class MyObject(@CompoundKey("sort", "2") @ColumnName("col1") column1: String, @CompoundKey("sort", "1") column2: String)

  case class TestDeltaUpdate(@ColumnName("col1") @CompoundKey("partition", "1") column1: Int, @CompoundKey("sort", "1") column2: String, value: Double)

  case class TestCompressionRepository(col1: String,
                                       col2: String,
                                       @Compress col3: Seq[InnerClass],
                                       @Compress col4: Seq[String]) {
  }

  case class TestCompressionRepositoryGZIP(col1: String,
                                           col2: String,
                                           @Compress(compressor = classOf[GZIPCompressor]) col3: Seq[InnerClass],
                                           @Compress(compressor = classOf[GZIPCompressor]) col4: Seq[String]) {
  }

  object ImplicitConverter {

    implicit val a2b: DatasetConverter[RepoAdapterTesterA, RepoAdapterTesterB] = new DatasetConverter[RepoAdapterTesterA, RepoAdapterTesterB] {

      implicit val encoderA: Encoder[RepoAdapterTesterA] = ExpressionEncoder[RepoAdapterTesterA]
      implicit val encoderB: Encoder[RepoAdapterTesterB] = ExpressionEncoder[RepoAdapterTesterB]

      override def convertFrom(ds: Dataset[RepoAdapterTesterB]): Dataset[RepoAdapterTesterA] =
        ds.drop("col3").as[RepoAdapterTesterA]

      override def convertTo(ds: Dataset[RepoAdapterTesterA]): Dataset[RepoAdapterTesterB] = {
        import org.apache.spark.sql.functions._
        ds.withColumn("col3", concat(col("col1"), col("col2"))).as[RepoAdapterTesterB]
      }
    }
  }

}
