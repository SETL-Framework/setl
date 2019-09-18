package com.jcdecaux.datacorp.spark.storage

import com.jcdecaux.datacorp.spark.annotation.ColumnName
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, Encoder}

package object repository {

  case class RepoAdapterTesterA(col1: String, col2: String)

  case class RepoAdapterTesterB(@ColumnName("column1") col1: String, col2: String, col3: String)

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
