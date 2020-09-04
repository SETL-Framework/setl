## Definition

[**Connector**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/connector/Connector.scala) is a non-typed abstraction of data access layer (DAL) that provides read/write functionalities for Spark *DataFrame*.

A basic connector is defined as follows:
```scala
trait Connector extends Logging {

  val spark: SparkSession

  val storage: Storage

  val reader: DataFrameReader

  var writer: DataFrameWriter[Row]

  def read(): DataFrame

  def write(t: DataFrame, suffix: Option[String] = None): Unit
}

```

The **Connector** trait was inherited by two abstract classes: **FileConnector** and **DBConnector**

## Implementation

[![](https://mermaid.ink/img/eyJjb2RlIjoiICBncmFwaCBURDtcblxuICBDb25uZWN0b3IgLS0-IEZpbGVDb25uZWN0b3I7XG4gIENvbm5lY3RvciAtLT4gREJDb25uZWN0b3I7XG5cbiAgRmlsZUNvbm5lY3RvciAtLT4gQ1NWQ29ubmVjdG9yO1xuICBGaWxlQ29ubmVjdG9yIC0tPiBKU09OQ29ubmVjdG9yO1xuICBDb25uZWN0b3IgLS0-IEV4Y2VsQ29ubmVjdG9yO1xuICBGaWxlQ29ubmVjdG9yIC0tPiBQYXJxdWV0Q29ubmVjdG9yO1xuXG4gIERCQ29ubmVjdG9yIC0tPiBDYXNzYW5kcmFDb25uZWN0b3I7XG4gIERCQ29ubmVjdG9yIC0tPiBEeW5hbW9EQkNvbm5lY3RvcjsiLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCJ9fQ)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoiICBncmFwaCBURDtcblxuICBDb25uZWN0b3IgLS0-IEZpbGVDb25uZWN0b3I7XG4gIENvbm5lY3RvciAtLT4gREJDb25uZWN0b3I7XG5cbiAgRmlsZUNvbm5lY3RvciAtLT4gQ1NWQ29ubmVjdG9yO1xuICBGaWxlQ29ubmVjdG9yIC0tPiBKU09OQ29ubmVjdG9yO1xuICBDb25uZWN0b3IgLS0-IEV4Y2VsQ29ubmVjdG9yO1xuICBGaWxlQ29ubmVjdG9yIC0tPiBQYXJxdWV0Q29ubmVjdG9yO1xuXG4gIERCQ29ubmVjdG9yIC0tPiBDYXNzYW5kcmFDb25uZWN0b3I7XG4gIERCQ29ubmVjdG9yIC0tPiBEeW5hbW9EQkNvbm5lY3RvcjsiLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCJ9fQ)

## FileConnector

[**FileConnector**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/connector/FileConnector.scala) could be used to access files stored in the different file systems

### Functionalities

A FileConnector could be instantiate as follows:
```scala
val fileConnector = new FileConnector(spark, options)
```
where `spark` is the current **SparkSession** and `options` is a `Map[String, String]` object.

#### Read

Read data from persistence storage. Need to be implemented in a concrete **FileConnector**.

#### Write

Write data to persistence storage. Need to be implemented in a concrete **FileConnector**.

#### Delete

Delete a file if the value of `path` defined in **options** is a file path. If `path` is a directory, then delete the directory with all its contents.

Use it with care!

#### Schema

The schema of data could be set by adding a key `schema` into the **options** map of the constructor. The schema must be a DDL format string:
> partition1 INT, partition2 STRING, clustering1 STRING, value LONG

#### Partition

Data could be partitioned before saving. To do this, call `partitionBy(columns: String*)` before `write(df)` and *Spark* will partition the *DataFrame* by creating subdirectories in the root directory.

#### Suffix

A suffix is similar to a partition, but it is defined manually while calling `write(df, suffix)`. **Connector** handles the suffix by creating a subdirectory with the same naming convention as Spark partition (by default it will be `_user_defined_suffix=suffix`.

>:warning: Currently (v0.3), you **can't** mix with-suffix write and non-suffix write when your data are partitioned. An **IllegalArgumentException** will be thrown in this case. The reason for which it's not supported is that, as suffix is handled by *Connector* and partition is handled by *Spark*, a suffix may confuse Spark when the latter tries to infer the structure of DataFrame.


#### Multiple files reading and name pattern matching

You can read multiple files at once if the `path` you set in **options** is a directory (instead of a file path). You can also filter files by setting a regex pattern `filenamePattern` in **options**.

#### File system support

- Local file system
- AWS S3
- Hadoop File System

#### S3 Authentication

To access S3, if *authentication error* occurs, you may have to provide extra settings in **options** for its authentication process. There are multiple authentication methods that could be set by changing **Authentication Providers**. 

To configure authentication, you can:
- add the following configurations to the argument **options** of the constructor.
- add the following configurations to `sparkSession.sparkContext.hadoopConfiguration`

The most common authentication providers we used are:
- `com.amazonaws.auth.DefaultAWSCredentialsProviderChain` for local development
- `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider` for local development
- `com.amazonaws.auth.InstanceProfileCredentialsProvider` for applications running in AWS environment.

To use `com.amazonaws.auth.DefaultAWSCredentialsProviderChain`, add the following configuration:

| key | value |
| ------ | ------ |
| fs.s3a.aws.credentials.provider | org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider |

And add **AWS_PROFILE=your_profile_name** into the environmental variables.

To use `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`:

| key | value |
| ------ | ------ |
| fs.s3a.aws.credentials.provider | org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider |
| fs.s3a.access.key | your_s3a_access_key | 
| fs.s3a.secret.key | your_s3a_secret_key | 
| fs.s3a.session.token | your_s3a_session_token | 

To use `com.amazonaws.auth.InstanceProfileCredentialsProvider`:

| key | value |
| ------ | ------ |
| fs.s3a.aws.credentials.provider | com.amazonaws.auth.InstanceProfileCredentialsProvider |

More information could be found [here](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#S3A_Authentication_methods)

## DBConnector

[DBConnector](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/connector/DBConnector.scala) could be used for accessing databases.

### Functionalities

#### Read

Read data from a database. Need to be implemented in a concrete **DBConnector**.

#### Create

Create a table in a database. Need to be implemented in a concrete **DBConnector**.

#### Write
Write data to a database. Need to be implemented in a concrete **DBConnector**.

#### Delete
Send a delete request.

## CSVConnector

### Options

| name  | default   |
| ------  |  ------- |
| path |   <user_input> |
| schema | <user_input> |
| filenamePattern* | <optional_input> |
| saveMode | <user_input> |
| sep  |  `,` |
| encoding   | `UTF-8`|
| quote  | `"`|
| header  | `false`|
| inferSchema | `false`|
| ignoreLeadingWhiteSpace  | `false`|
| ignoreTrailingWhiteSpace  | `false`|
| nullValue  | empty string|
| emptyValue  | `false`|
| dateFormat  | `yyyy-MM-dd`|
| timestampFormat  | `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`|

For other options, please refer to [this doc](https://docs.databricks.com/spark/latest/data-sources/read-csv.html).

## JSONConnector

### Options

| name  | default |
| ------  |  ------- |
| path | <user_input>  |
| schema | <user_input>  |
| filenamePattern* | <optional_input>  |
| saveMode | <user_input>  |
| primitivesAsString  |  `false` |
| prefersDecimal   | `false`|
| allowComments  | `false`|
| allowUnquotedFieldNames  | `false`|
| allowSingleQuotes | `true`|
| allowNumericLeadingZeros  | `false`|
| multiLine  | `false`|
| encoding  | not set|
| lineSep  | default covers all `\r`, `\r\n` and `\n`|
| dateFormat  | `yyyy-MM-dd`|
| timestampFormat  | `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`|
|dropFieldIfAllNull| `false`|
 

## ParquetConnector

### Options

| name  | default |
| ------  |  ------- |
| path | <user_input>  |
| filenamePattern* | <optional_input>  |
| saveMode | <user_input>  |

## ExcelConnector
### Options

| name  | default |
| ------  |  ------- |
| path | <user_input>  |
| filenamePattern* | <optional_input>  |
| schema | <user_input>  |
| saveMode | <user_input>  |
| useHeader | <user_input>  |
| dataAddress | `A1` |
| treatEmptyValuesAsNulls | `true` |
| inferSchema | `false` |
| addColorColumns | `false` |
| dateFormat  | `yyyy-MM-dd` |
| timestampFormat  | `yyyy-mm-dd hh:mm:ss.000` |
| maxRowsInMemory  | `None` |
| excerptSize  | 10 |
| workbookPassword  | `None` |

## DynamoDBConnector

### Options

| name  | default |
| ------  |  ------- |
| region | <user_input>  |
| table | <user_input>  |
| saveMode | <user_input>  |

## CassandraConnector

### Options

| name  | default |
| ------  |  ------- |
| keyspace | <user_input>  |
| table | <user_input>  |
| partitionKeyColumns | `None` |
| clusteringKeyColumns | `None` |
