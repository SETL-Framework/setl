## 0.2.5 (2019-06-12)
- Added DynamoDB V2 repository
- Added auxiliary constructors of case class `Condition`
- Added SchemaConverter
  ```scala
  import com.jcdecaux.datacorp.spark.annotations.colName
  case class MyObject(@colName("col1") column1: String, column2: String)

  val ds: Dataset[MyObject] = Seq(MyObject("a", "A"), MyObject("b", "B")).toDS()
  // +-------+-------+
  // |column1|column2|
  // +-------+-------+
  // |      a|      A|
  // |      b|      B|
  // +-------+-------+

  val df = SchemaConverter.toDF(ds)
  // +----+-------+
  // |col1|column2|
  // +----+-------+
  // |   a|      A|
  // |   b|      B|
  // +----+-------+

  val ds2 = SchemaConverter.fromDF[MyObject](df)
  // +-------+-------+
  // |column1|column2|
  // +-------+-------+
  // |      a|      A|
  // |      b|      B|
  // +-------+-------+
  ```

## 0.2.4 (2019-06-11)
- Added DynamoDB Repository

## 0.2.3 (2019-06-11)
- Removed scope provided from connectors and TypeSafe config

## 0.2.2 (2019-06-11)
- Added DynamoDB Connector

## 0.2.1 (2019-06-03)
- Removed unnecessary Type variable in `Connector` 
- Added `ConnectorBuilder` to directly build a connector from a typesafe's `Config` object
- Added auxiliary constructor in `SparkRepositoryBuilder` so we can create a repository like this: 
  ```scala
  val repo = new SparkRepositoryBuilder[TestObject](config).setSpark(spark).build().get()
  ```
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