## Definition

[**Connector**](https://github.com/SETL-Developers/setl/tree/master/src/main/scala/com/jcdecaux/setl/storage/connector/Connector.scala) 
is a non-typed abstraction of data access layer (DAL) that provides read/write functionalities for Spark 
*DataFrame*.

A basic connector is defined as follows:

```scala
trait Connector extends HasSparkSession with Logging {

  val storage: Storage

  def read(): DataFrame

  def write(t: DataFrame, suffix: Option[String]): Unit

  def write(t: DataFrame): Unit

}
```

## Implementation

The **Connector** trait should be inherited by other concrete connectors. For example: 
**FileConnector**, **DBConnector**, **ACIDConnector**, etc.

> Starting from SETL 3.0, one should inherit the trait `ConnectorInterface` in the implementation of 
> new connectors.
>
> For more detail, see [this article](CustomConnector)

## Class diagram

[![](https://mermaid.ink/img/eyJjb2RlIjoiICBncmFwaCBMUjtcblxuICBDb25uZWN0b3IgLS0tIEZpbGVDb25uZWN0b3I7XG4gIENvbm5lY3RvciAtLS0gREJDb25uZWN0b3I7XG4gIENvbm5lY3RvciAtLS0gQUNJRENvbm5lY3RvcjtcbiAgQ29ubmVjdG9yIC0tLSBFeGNlbENvbm5lY3RvcjtcblxuICBGaWxlQ29ubmVjdG9yIC0tLSBDU1ZDb25uZWN0b3I7XG4gIEZpbGVDb25uZWN0b3IgLS0tIEpTT05Db25uZWN0b3I7XG4gIEZpbGVDb25uZWN0b3IgLS0tIFBhcnF1ZXRDb25uZWN0b3I7XG5cbiAgREJDb25uZWN0b3IgLS0tIENhc3NhbmRyYUNvbm5lY3RvcjtcbiAgREJDb25uZWN0b3IgLS0tIER5bmFtb0RCQ29ubmVjdG9yO1xuICBEQkNvbm5lY3RvciAtLS0gSkRCQ0Nvbm5lY3RvcjtcblxuICBBQ0lEQ29ubmVjdG9yIC0tLSBEZWx0YUNvbm5lY3RvcjsiLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCIsInRoZW1lVmFyaWFibGVzIjp7ImJhY2tncm91bmQiOiJ3aGl0ZSIsInByaW1hcnlDb2xvciI6IiNFQ0VDRkYiLCJzZWNvbmRhcnlDb2xvciI6IiNmZmZmZGUiLCJ0ZXJ0aWFyeUNvbG9yIjoiaHNsKDgwLCAxMDAlLCA5Ni4yNzQ1MDk4MDM5JSkiLCJwcmltYXJ5Qm9yZGVyQ29sb3IiOiJoc2woMjQwLCA2MCUsIDg2LjI3NDUwOTgwMzklKSIsInNlY29uZGFyeUJvcmRlckNvbG9yIjoiaHNsKDYwLCA2MCUsIDgzLjUyOTQxMTc2NDclKSIsInRlcnRpYXJ5Qm9yZGVyQ29sb3IiOiJoc2woODAsIDYwJSwgODYuMjc0NTA5ODAzOSUpIiwicHJpbWFyeVRleHRDb2xvciI6IiMxMzEzMDAiLCJzZWNvbmRhcnlUZXh0Q29sb3IiOiIjMDAwMDIxIiwidGVydGlhcnlUZXh0Q29sb3IiOiJyZ2IoOS41MDAwMDAwMDAxLCA5LjUwMDAwMDAwMDEsIDkuNTAwMDAwMDAwMSkiLCJsaW5lQ29sb3IiOiIjMzMzMzMzIiwidGV4dENvbG9yIjoiIzMzMyIsIm1haW5Ca2ciOiIjRUNFQ0ZGIiwic2Vjb25kQmtnIjoiI2ZmZmZkZSIsImJvcmRlcjEiOiIjOTM3MERCIiwiYm9yZGVyMiI6IiNhYWFhMzMiLCJhcnJvd2hlYWRDb2xvciI6IiMzMzMzMzMiLCJmb250RmFtaWx5IjoiXCJ0cmVidWNoZXQgbXNcIiwgdmVyZGFuYSwgYXJpYWwiLCJmb250U2l6ZSI6IjE2cHgiLCJsYWJlbEJhY2tncm91bmQiOiIjZThlOGU4Iiwibm9kZUJrZyI6IiNFQ0VDRkYiLCJub2RlQm9yZGVyIjoiIzkzNzBEQiIsImNsdXN0ZXJCa2ciOiIjZmZmZmRlIiwiY2x1c3RlckJvcmRlciI6IiNhYWFhMzMiLCJkZWZhdWx0TGlua0NvbG9yIjoiIzMzMzMzMyIsInRpdGxlQ29sb3IiOiIjMzMzIiwiZWRnZUxhYmVsQmFja2dyb3VuZCI6IiNlOGU4ZTgiLCJhY3RvckJvcmRlciI6ImhzbCgyNTkuNjI2MTY4MjI0MywgNTkuNzc2NTM2MzEyOCUsIDg3LjkwMTk2MDc4NDMlKSIsImFjdG9yQmtnIjoiI0VDRUNGRiIsImFjdG9yVGV4dENvbG9yIjoiYmxhY2siLCJhY3RvckxpbmVDb2xvciI6ImdyZXkiLCJzaWduYWxDb2xvciI6IiMzMzMiLCJzaWduYWxUZXh0Q29sb3IiOiIjMzMzIiwibGFiZWxCb3hCa2dDb2xvciI6IiNFQ0VDRkYiLCJsYWJlbEJveEJvcmRlckNvbG9yIjoiaHNsKDI1OS42MjYxNjgyMjQzLCA1OS43NzY1MzYzMTI4JSwgODcuOTAxOTYwNzg0MyUpIiwibGFiZWxUZXh0Q29sb3IiOiJibGFjayIsImxvb3BUZXh0Q29sb3IiOiJibGFjayIsIm5vdGVCb3JkZXJDb2xvciI6IiNhYWFhMzMiLCJub3RlQmtnQ29sb3IiOiIjZmZmNWFkIiwibm90ZVRleHRDb2xvciI6ImJsYWNrIiwiYWN0aXZhdGlvbkJvcmRlckNvbG9yIjoiIzY2NiIsImFjdGl2YXRpb25Ca2dDb2xvciI6IiNmNGY0ZjQiLCJzZXF1ZW5jZU51bWJlckNvbG9yIjoid2hpdGUiLCJzZWN0aW9uQmtnQ29sb3IiOiJyZ2JhKDEwMiwgMTAyLCAyNTUsIDAuNDkpIiwiYWx0U2VjdGlvbkJrZ0NvbG9yIjoid2hpdGUiLCJzZWN0aW9uQmtnQ29sb3IyIjoiI2ZmZjQwMCIsInRhc2tCb3JkZXJDb2xvciI6IiM1MzRmYmMiLCJ0YXNrQmtnQ29sb3IiOiIjOGE5MGRkIiwidGFza1RleHRMaWdodENvbG9yIjoid2hpdGUiLCJ0YXNrVGV4dENvbG9yIjoid2hpdGUiLCJ0YXNrVGV4dERhcmtDb2xvciI6ImJsYWNrIiwidGFza1RleHRPdXRzaWRlQ29sb3IiOiJibGFjayIsInRhc2tUZXh0Q2xpY2thYmxlQ29sb3IiOiIjMDAzMTYzIiwiYWN0aXZlVGFza0JvcmRlckNvbG9yIjoiIzUzNGZiYyIsImFjdGl2ZVRhc2tCa2dDb2xvciI6IiNiZmM3ZmYiLCJncmlkQ29sb3IiOiJsaWdodGdyZXkiLCJkb25lVGFza0JrZ0NvbG9yIjoibGlnaHRncmV5IiwiZG9uZVRhc2tCb3JkZXJDb2xvciI6ImdyZXkiLCJjcml0Qm9yZGVyQ29sb3IiOiIjZmY4ODg4IiwiY3JpdEJrZ0NvbG9yIjoicmVkIiwidG9kYXlMaW5lQ29sb3IiOiJyZWQiLCJsYWJlbENvbG9yIjoiYmxhY2siLCJlcnJvckJrZ0NvbG9yIjoiIzU1MjIyMiIsImVycm9yVGV4dENvbG9yIjoiIzU1MjIyMiIsImNsYXNzVGV4dCI6IiMxMzEzMDAiLCJmaWxsVHlwZTAiOiIjRUNFQ0ZGIiwiZmlsbFR5cGUxIjoiI2ZmZmZkZSIsImZpbGxUeXBlMiI6ImhzbCgzMDQsIDEwMCUsIDk2LjI3NDUwOTgwMzklKSIsImZpbGxUeXBlMyI6ImhzbCgxMjQsIDEwMCUsIDkzLjUyOTQxMTc2NDclKSIsImZpbGxUeXBlNCI6ImhzbCgxNzYsIDEwMCUsIDk2LjI3NDUwOTgwMzklKSIsImZpbGxUeXBlNSI6ImhzbCgtNCwgMTAwJSwgOTMuNTI5NDExNzY0NyUpIiwiZmlsbFR5cGU2IjoiaHNsKDgsIDEwMCUsIDk2LjI3NDUwOTgwMzklKSIsImZpbGxUeXBlNyI6ImhzbCgxODgsIDEwMCUsIDkzLjUyOTQxMTc2NDclKSJ9fSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoiICBncmFwaCBMUjtcblxuICBDb25uZWN0b3IgLS0tIEZpbGVDb25uZWN0b3I7XG4gIENvbm5lY3RvciAtLS0gREJDb25uZWN0b3I7XG4gIENvbm5lY3RvciAtLS0gQUNJRENvbm5lY3RvcjtcbiAgQ29ubmVjdG9yIC0tLSBFeGNlbENvbm5lY3RvcjtcblxuICBGaWxlQ29ubmVjdG9yIC0tLSBDU1ZDb25uZWN0b3I7XG4gIEZpbGVDb25uZWN0b3IgLS0tIEpTT05Db25uZWN0b3I7XG4gIEZpbGVDb25uZWN0b3IgLS0tIFBhcnF1ZXRDb25uZWN0b3I7XG5cbiAgREJDb25uZWN0b3IgLS0tIENhc3NhbmRyYUNvbm5lY3RvcjtcbiAgREJDb25uZWN0b3IgLS0tIER5bmFtb0RCQ29ubmVjdG9yO1xuICBEQkNvbm5lY3RvciAtLS0gSkRCQ0Nvbm5lY3RvcjtcblxuICBBQ0lEQ29ubmVjdG9yIC0tLSBEZWx0YUNvbm5lY3RvcjsiLCJtZXJtYWlkIjp7InRoZW1lIjoiZGVmYXVsdCIsInRoZW1lVmFyaWFibGVzIjp7ImJhY2tncm91bmQiOiJ3aGl0ZSIsInByaW1hcnlDb2xvciI6IiNFQ0VDRkYiLCJzZWNvbmRhcnlDb2xvciI6IiNmZmZmZGUiLCJ0ZXJ0aWFyeUNvbG9yIjoiaHNsKDgwLCAxMDAlLCA5Ni4yNzQ1MDk4MDM5JSkiLCJwcmltYXJ5Qm9yZGVyQ29sb3IiOiJoc2woMjQwLCA2MCUsIDg2LjI3NDUwOTgwMzklKSIsInNlY29uZGFyeUJvcmRlckNvbG9yIjoiaHNsKDYwLCA2MCUsIDgzLjUyOTQxMTc2NDclKSIsInRlcnRpYXJ5Qm9yZGVyQ29sb3IiOiJoc2woODAsIDYwJSwgODYuMjc0NTA5ODAzOSUpIiwicHJpbWFyeVRleHRDb2xvciI6IiMxMzEzMDAiLCJzZWNvbmRhcnlUZXh0Q29sb3IiOiIjMDAwMDIxIiwidGVydGlhcnlUZXh0Q29sb3IiOiJyZ2IoOS41MDAwMDAwMDAxLCA5LjUwMDAwMDAwMDEsIDkuNTAwMDAwMDAwMSkiLCJsaW5lQ29sb3IiOiIjMzMzMzMzIiwidGV4dENvbG9yIjoiIzMzMyIsIm1haW5Ca2ciOiIjRUNFQ0ZGIiwic2Vjb25kQmtnIjoiI2ZmZmZkZSIsImJvcmRlcjEiOiIjOTM3MERCIiwiYm9yZGVyMiI6IiNhYWFhMzMiLCJhcnJvd2hlYWRDb2xvciI6IiMzMzMzMzMiLCJmb250RmFtaWx5IjoiXCJ0cmVidWNoZXQgbXNcIiwgdmVyZGFuYSwgYXJpYWwiLCJmb250U2l6ZSI6IjE2cHgiLCJsYWJlbEJhY2tncm91bmQiOiIjZThlOGU4Iiwibm9kZUJrZyI6IiNFQ0VDRkYiLCJub2RlQm9yZGVyIjoiIzkzNzBEQiIsImNsdXN0ZXJCa2ciOiIjZmZmZmRlIiwiY2x1c3RlckJvcmRlciI6IiNhYWFhMzMiLCJkZWZhdWx0TGlua0NvbG9yIjoiIzMzMzMzMyIsInRpdGxlQ29sb3IiOiIjMzMzIiwiZWRnZUxhYmVsQmFja2dyb3VuZCI6IiNlOGU4ZTgiLCJhY3RvckJvcmRlciI6ImhzbCgyNTkuNjI2MTY4MjI0MywgNTkuNzc2NTM2MzEyOCUsIDg3LjkwMTk2MDc4NDMlKSIsImFjdG9yQmtnIjoiI0VDRUNGRiIsImFjdG9yVGV4dENvbG9yIjoiYmxhY2siLCJhY3RvckxpbmVDb2xvciI6ImdyZXkiLCJzaWduYWxDb2xvciI6IiMzMzMiLCJzaWduYWxUZXh0Q29sb3IiOiIjMzMzIiwibGFiZWxCb3hCa2dDb2xvciI6IiNFQ0VDRkYiLCJsYWJlbEJveEJvcmRlckNvbG9yIjoiaHNsKDI1OS42MjYxNjgyMjQzLCA1OS43NzY1MzYzMTI4JSwgODcuOTAxOTYwNzg0MyUpIiwibGFiZWxUZXh0Q29sb3IiOiJibGFjayIsImxvb3BUZXh0Q29sb3IiOiJibGFjayIsIm5vdGVCb3JkZXJDb2xvciI6IiNhYWFhMzMiLCJub3RlQmtnQ29sb3IiOiIjZmZmNWFkIiwibm90ZVRleHRDb2xvciI6ImJsYWNrIiwiYWN0aXZhdGlvbkJvcmRlckNvbG9yIjoiIzY2NiIsImFjdGl2YXRpb25Ca2dDb2xvciI6IiNmNGY0ZjQiLCJzZXF1ZW5jZU51bWJlckNvbG9yIjoid2hpdGUiLCJzZWN0aW9uQmtnQ29sb3IiOiJyZ2JhKDEwMiwgMTAyLCAyNTUsIDAuNDkpIiwiYWx0U2VjdGlvbkJrZ0NvbG9yIjoid2hpdGUiLCJzZWN0aW9uQmtnQ29sb3IyIjoiI2ZmZjQwMCIsInRhc2tCb3JkZXJDb2xvciI6IiM1MzRmYmMiLCJ0YXNrQmtnQ29sb3IiOiIjOGE5MGRkIiwidGFza1RleHRMaWdodENvbG9yIjoid2hpdGUiLCJ0YXNrVGV4dENvbG9yIjoid2hpdGUiLCJ0YXNrVGV4dERhcmtDb2xvciI6ImJsYWNrIiwidGFza1RleHRPdXRzaWRlQ29sb3IiOiJibGFjayIsInRhc2tUZXh0Q2xpY2thYmxlQ29sb3IiOiIjMDAzMTYzIiwiYWN0aXZlVGFza0JvcmRlckNvbG9yIjoiIzUzNGZiYyIsImFjdGl2ZVRhc2tCa2dDb2xvciI6IiNiZmM3ZmYiLCJncmlkQ29sb3IiOiJsaWdodGdyZXkiLCJkb25lVGFza0JrZ0NvbG9yIjoibGlnaHRncmV5IiwiZG9uZVRhc2tCb3JkZXJDb2xvciI6ImdyZXkiLCJjcml0Qm9yZGVyQ29sb3IiOiIjZmY4ODg4IiwiY3JpdEJrZ0NvbG9yIjoicmVkIiwidG9kYXlMaW5lQ29sb3IiOiJyZWQiLCJsYWJlbENvbG9yIjoiYmxhY2siLCJlcnJvckJrZ0NvbG9yIjoiIzU1MjIyMiIsImVycm9yVGV4dENvbG9yIjoiIzU1MjIyMiIsImNsYXNzVGV4dCI6IiMxMzEzMDAiLCJmaWxsVHlwZTAiOiIjRUNFQ0ZGIiwiZmlsbFR5cGUxIjoiI2ZmZmZkZSIsImZpbGxUeXBlMiI6ImhzbCgzMDQsIDEwMCUsIDk2LjI3NDUwOTgwMzklKSIsImZpbGxUeXBlMyI6ImhzbCgxMjQsIDEwMCUsIDkzLjUyOTQxMTc2NDclKSIsImZpbGxUeXBlNCI6ImhzbCgxNzYsIDEwMCUsIDk2LjI3NDUwOTgwMzklKSIsImZpbGxUeXBlNSI6ImhzbCgtNCwgMTAwJSwgOTMuNTI5NDExNzY0NyUpIiwiZmlsbFR5cGU2IjoiaHNsKDgsIDEwMCUsIDk2LjI3NDUwOTgwMzklKSIsImZpbGxUeXBlNyI6ImhzbCgxODgsIDEwMCUsIDkzLjUyOTQxMTc2NDclKSJ9fSwidXBkYXRlRWRpdG9yIjpmYWxzZX0)

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

## Configuration example

[Example](configuration_example)
