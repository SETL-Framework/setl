# RepositoryAdapter

In some situation, the data format defined in the data source doesn't match the case class defined in our project, and we want to hide 
the conversion detail (which may be irrelevant to the business logic). We can achieve this by using the 
[SparkRepositoryAdapter](https://github.com/SETL-Developers/setl/blob/master/src/main/scala/com/jcdecaux/setl/storage/repository/ImplicitRepositoryAdapter.scala).
and [DatasetConverter](https://github.com/SETL-Developers/setl/blob/master/src/main/scala/com/jcdecaux/setl/storage/DatasetConverter.scala)

## Example

Imagine our datasource has a format that match the following case class:

```scala
case class DataSourceFormat(col1: String, col2: Int, col3: String)

// col1, col2, col3
//   r1,    1, r1-1
//   r2,    2, r1-2
```

The column `col3` is not necessary (as it's only a concatenation of `col1` and `col2`, we can ignore it and use this 
case class in out project:

```scala
case class ProjectFormat(col1: String, col2: Int)
```

So the data conversions that we want to hide are:
- during the reading, we want to implicitly drop the `col3`
- during the writing, we want to implicitly create `col3` by concatenating `col1` and `col2`

Let's implement our dataset converter:
```scala
import io.github.setl.storage.DatasetConverter

implicit val myConverter = new DatasetConverter[ProjectFormat, DataSourceFormat] {
  override def convertFrom(t2: Dataset[DataSourceFormat]): Dataset[ProjectFormat] = {
    t2.drop("col3")
      .as[ProjectFormat](ExpressionEncoder[ProjectFormat])
  }

  override def convertTo(t1: Dataset[ProjectFormat]): Dataset[DataSourceFormat] = {
    import org.apache.spark.sql.functions._

    t1.withColumn("col3", concat(col("col1"), lit("-"), col("col2")))
      .as[DataSourceFormat](ExpressionEncoder[DataSourceFormat])
  }
}
```

To use this converter:
```scala
import io.github.setl.storage.repository.ImplicitRepositoryAdapter._

// Supposed that we have a repository of type ProjectFormat.
// After the import, several new methods will be added to the SparkRepository
// For example: convertAndSave and findAllAndConvert
val projectFormatRepo = SparkRepository[ProjectFormat]

// This will convert a Dataset[ProjectFormat] to a Dataset[DataSourceFormat] and save it 
projectFormatRepo.convertAndSave(projectFormatDataset)

// This will load a Dataset[DataSourceFormat] and automatically convert it to a Dataset[ProjectFormat] 
val loaded = projectFormatRepo.findAllAndConvert()
```
