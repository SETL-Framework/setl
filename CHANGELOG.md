## 0.3.2-SNAPSHOT (2019-10-02)
- Added **@Compress** annotation. **SparkRepository** will compress all columns having this annotation by
using a **Compressor** (the default compressor is **XZCompressor**)
```scala
case class CompressionDemo(@Compress col1: Seq[Int],
                           @Compress(compressor = classOf[GZIPCompressor]) col2: Seq[String])
```
- Added interface **Compressor** and implemented **XZCompressor** and **GZIPCompressor**
- Added **SparkRepositoryAdapter[A, B]**. It will allow a **SparkRepository[A]** to write/read a data store of type
 **B** by using an implicit **DatasetConverter[A, B]**
- Added trait **Converter[A, B]** that handles the conversion between an object of type A and an object of type **B**
- Added abstract class **DatasetConverter[A, B]** that extends a **Converter[Dataset[A], Dataset[B]]**
- Added auto-correction for `SparkRepository.findby(conditions)` method when we filter by case class field name instead of column name
- Added **DCContext** that simplifies the creation of *SparkSession*, *SparkRepository*, *Connector* and *Pipeline*
- Added a builder for **ConfigLoader** to simplify the instantiation of a **ConfigLoader** object

## 0.3.1 (2019-08-23)
- Added sequential mode in class `Stage`. Use can turn in on by setting `parallel` to *true*.
- Added external data flow description in pipeline description
- Added method `beforeAll` into `ConfigLoader`
- Added new method `addStage` and `addFactory` that take a class object as input. The instantiation will be handled 
  by the stage.
- Removed implicit argument encoder from all methods of Repository trait
- Added new get method to **Pipeline**: `get[A](cls: Class[_ <: Factory[_]): A`. 

## 0.3.0 (2019-07-22)

#### New Features
- Added `Delivery` annotation to handle inputs of a Factory
  ```scala
  class Foo {
    @Delivery(producer = classOf[Factory1], optional = true)
    var input1: String = _
  
    @Delivery(producer = classOf[Factory2])
    var input2: String = _
  }
  ```
- Added an optional argument `suffix` in `FileConnector` and `SparkRepository`
- Added method `partitionBy` in `FileConnector` and `SparkRepository`
- Added possibility to filter by name pattern when a FileConnector is trying to read a directory. 
  To do this, add `filenamePattern` into the configuration file
- Added possibility to create a `Conf` object from Map. 
  ```scala
  Conf(Map("a" -> "A"))
  ```
- Improved Hadoop and S3 compatibility of connectors

#### Developper Features
- Added `DispatchManager` class. It will dispatch its deliverable object to setters (denoted by @Delivery) of a factory
- Added `Deliverable` class, which contains a payload to be delivered
- Added `PipelineInspector` to describe a pipeline
- Added `FileConnector` and `DBConnector`

#### Fixed Issue
- Fixed issue of file path containing whitespace character(s) in the URI creation (52eee322aacd85e0b03a96435b07c4565e894934)

#### Other changes
- Removed `EnrichedConnector`
- Removed V1 interfaces

## 0.2.8 (2019-07-09)
- Added a second argument to CompoundKey to handle primary and sort keys

## 0.2.7 (2019-06-21)
- Added `Conf` into `SparkRepositoryBuilder` and changed all the set methods 
of `SparkRepositoryBuilder` to use the conf object
- Changed package name `com.jcdecaux.datacorp.spark.annotations` to `com.jcdecaux.datacorp.spark.annotation`

## 0.2.6 (2019-06-18)
- Added annotation `ColumnName`, which could be used to replace the current column name 
with an alias in the data storage.
- Added annotation `CompoundKey`. It could be used to define a compound key for databases 
that only allow one partition key
- Added sheet name into arguments of ExcelConnector

## 0.2.5 (2019-06-12)
- Added DynamoDB V2 repository
- Added auxiliary constructors of case class `Condition`
- Added SchemaConverter

## 0.2.4 (2019-06-11)
- Added DynamoDB Repository

## 0.2.3 (2019-06-11)
- Removed scope provided from connectors and TypeSafe config

## 0.2.2 (2019-06-11)
- Added DynamoDB Connector

## 0.2.1 (2019-06-03)
- Removed unnecessary Type variable in `Connector` 
- Added `ConnectorBuilder` to directly build a connector from a typesafe's `Config` object
- Added auxiliary constructor in `SparkRepositoryBuilder`
- Added enumeration `AppEnv`

## 0.2.0 (2019-05-21)
- Changed spark version to 2.4.3
- Added `SparkRepositoryBuilder` that allows creation of a `SparkRepository` for a given class without creating a 
dedicated `Repository` class
- Added Excel support for `SparkRepository` by creating `ExcelConnector`
- Added `Logging` trait

## 0.1.6 (2019-04-25)
- Fixed `Factory` class covariance issue (0764d10d616c3171d9bfd58acfffafbd8b9dda15)
- Added documentation

## 0.1.5 (2019-04-23)
- Added changelog
- Changed `.gitlab-ci.yml` to speed up CI

## 0.1.4 (2019-04-19) 
- Added unit tests
- Added `.gitlab-ci.yml`
