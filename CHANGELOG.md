## 0.4.3 (2020-03-26)
Changes:
- Updated spark-cassandra-connector from 2.4.2 to 2.5.0
- Updated spark-excel-connector from 0.12.4 to 0.13.1
- Updated spark-dynamodb-connector from 1.0.1 to 1.0.4
- Updated scalatest (scope test) from 3.1.0 to 3.1.2
- Updated postgresql (scope test) from 42.2.9 to 42.2.12

New Features:
- Added pipeline dependency check before starting the spark job (#113)
- Added default Spark job group and description (#115)

Fixes
- Fixed path separator in FileConnectorSuite

## 0.4.2 (2020-02-15)
- Fixed cross building issue (#111)

## 0.4.1 (2020-02-13)
Changes:
- Changed benchmark unit of time to *seconds* (#88)

Fixes:
- The master URL of SparkSession can now be overwritten in local environment (#74)
- `FileConnector` now lists path correctly for nested directories (#97)

New features:
- Added [Mermaid](https://mermaidjs.github.io/#/) diagram generation to **Pipeline** (#51)
- Added `showDiagram()` method to **Pipeline** that prints the Mermaid code and generates the live editor URL 🎩🐰✨ (#52)
- Added **Codecov** report and **Scala API doc** 
- Added `delete` method in `JDBCConnector` (#82)
- Added `drop` method in `DBConnector` (#83)
- Added support for both of the following two Spark configuration styles in SETL builder (#86)
  ```hocon
  setl.config {
    spark {
      spark.app.name = "my_app"
      spark.sql.shuffle.partitions = "1000"
    }
  }
  
  setl.config_2 {
    spark.app.name = "my_app"
    spark.sql.shuffle.partitions = "1000"
  }
  ```

Others:
- Improved test coverage

## 0.4.0 (2020-01-09)
Changes: 
- BREAKING CHANGE: Renamed **DCContext** to **Setl**
- Changed the default application environment config path into **setl.environment**
- Changed the default context config path into **setl.config**

Fixes:
- Fixed issue of DynamoDBConnector that doesn't take user configuration
- Fixed issue of CompoundKey annotation. Now SparkRepository handles correctly columns having 
  multiple compound keys. (#36)

New features:
- Added support for private variable delivery (#24)
- Added empty SparkRepository as placeholder (#30)
- Added annotation **Benchmark** that could be used on methods of an **AbstractFactory** (#35)

Others:
- Optimized **DeliverableDispatcher**
- Optimized **PipelineInspector** (#33)

## 0.3.5 (2019-12-16)
- BREAKING CHANGE: replace the Spark compatible version by the Scala compatible version in the artifact ID. The old artifact id **dc-spark-sdk_2.4** was changed to **dc-spark-sdk_2.11** (or **dc-spark-sdk_2.12**)
- Upgraded dependencies
- Added Scala 2.12 support
- Removed **SparkSession** from Connector and SparkRepository constructor (old constructors are kept but now deprecated)
- Added **Column** type support in FindBy method of **SparkRepository** and **Condition**
- Added method **setConnector** and **setRepository** in **Setl** that accept object of type Connector/SparkRepository

## 0.3.4 (2019-12-06)
- Added read cache into spark repository to avoid consecutive disk IO.
- Added option **autoLoad** in the Delivery annotation so that *DeliverableDispatcher* can still handle the dependency injection in the case where the delivery is missing but a corresponding repository is present.
- Added option **condition** in the Delivery annotation to pre-filter loaded data when **autoLoad** is set to true.
- Added option **id** in the Delivery annotation. DeliveryDispatcher will match deliveries by the id in addition to the payload type. By default the id is an empty string ("").
- Added **setConnector** method in DCContext. Each connector should be delivered with an ID. By default the ID will be itsconfig path.
- Added support of wildcard path for SparkRepository and Connector
- Added JDBCConnector

## 0.3.3 (2019-10-22)
- Added **SnappyCompressor**.
- Added method **persist(persistence: Boolean)** into **Stage** and **Factory** to activate/deactivate output persistence. By default the output persistence is set to *true*.
- Added implicit method `filter(cond: Set[Condition])` for Dataset and DataFrame.
- Added `setUserDefinedSuffixKey` and `getUserDefinedSuffixKey` to **SparkRepository**.

## 0.3.2 (2019-10-14)
- Added **@Compress** annotation. **SparkRepository** will compress all columns having this annotation by using a **Compressor** (the default compressor is **XZCompressor**)
```scala
case class CompressionDemo(@Compress col1: Seq[Int],
                           @Compress(compressor = classOf[GZIPCompressor]) col2: Seq[String])
```

- Added interface **Compressor** and implemented **XZCompressor** and **GZIPCompressor**
- Added **SparkRepositoryAdapter[A, B]**. It will allow a **SparkRepository[A]** to write/read a data store of type **B** by using an implicit **DatasetConverter[A, B]**
- Added trait **Converter[A, B]** that handles the conversion between an object of type A and an object of type **B**
- Added abstract class **DatasetConverter[A, B]** that extends a **Converter[Dataset[A], Dataset[B]]**
- Added auto-correction for `SparkRepository.findby(conditions)` method when we filter by case class field name instead of column name
- Added **DCContext** that simplifies the creation of *SparkSession*, *SparkRepository*, *Connector* and *Pipeline*
- Added a builder for **ConfigLoader** to simplify the instantiation of a **ConfigLoader** object
- Added `readStandardJSON` and `writeStandardJSON` method into **JSONConnector** to read/write standard JSON format file

## 0.3.1 (2019-08-23)
- Added sequential mode in class `Stage`. Use can turn in on by setting `parallel` to *true*.
- Added external data flow description in pipeline description
- Added method `beforeAll` into `ConfigLoader`
- Added new method `addStage` and `addFactory` that take a class object as input. The instantiation will be handled by the stage.
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
- Added possibility to filter by name pattern when a FileConnector is trying to read a directory. To do this, add `filenamePattern` into the configuration file
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
- Added `Conf` into `SparkRepositoryBuilder` and changed all the set methods of `SparkRepositoryBuilder` to use the conf object
- Changed package name `com.jcdecaux.setl.annotations` to `com.jcdecaux.setl.annotation`

## 0.2.6 (2019-06-18)
- Added annotation `ColumnName`, which could be used to replace the current column name with an alias in the data storage.
- Added annotation `CompoundKey`. It could be used to define a compound key for databases that only allow one partition key
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
- Added `SparkRepositoryBuilder` that allows creation of a `SparkRepository` for a given class without creating a dedicated `Repository` class
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
