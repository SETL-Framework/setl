## CSV

### Options

| name    | description | optional | example  |
| ------  |  ---------- |  ------- |  ------- |
| path | directory of parquet files | false  |  |
| schema | Specifies the schema by using the input DDL-formatted string | true | `c1 STRING, c2 INT` |
| saveMode |  |  true, default `ErrorIfExists` | `Append` |
| filenamePattern | Regex of the names of file to be read. When `filenamePattern` is set, then this connector can only be used for reading data | true, default empty | `(file)(.*)(\\.csv)` |
| sep  | separator of csv file | true, default `,`  | `;` |
| encoding | | true, default `UTF-8`   | `UTF-8`|
| quote  | a single character used for escaping quoted values | true, default `"`|  | 
| header | uses the first line as names of columns | true, default `false`|  |
| inferSchema | infers the input schema automatically from data | true, default `false`| |
| ignoreLeadingWhiteSpace | | true, default `false`| |
| ignoreTrailingWhiteSpace  | | true, default `false`| |
| nullValue | sets the string representation of a null value | true, default empty string|  |
| emptyValue | sets the string representation of an empty value | true, default empty string|  |
| dateFormat  | sets the string that indicates a date format | true, default `yyyy-MM-dd`| |
| timestampFormat | sets the string that indicates a timestamp format  | true, default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`| |
| pathFormat | choose between REGEX path format (ex: `^s3:\/\/bucket\/\/col1=B\/col2=([A-C])$`) or WILDCARD path format (ex: `s3://bucket/*/*A*/internal-*.csv`) | true, default `REGEX`| `WILDCARD` |

For other options, please refer to [this doc](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html).

### Example

```
csv {
  storage = "CSV"
  path = "src/test/resources/csv_dir"
  inferSchema = "true"
  sep = ";"
  header = "true"
  saveMode = "Append"
}
```

```
csvWithSchema {
  storage = "CSV"
  path = "src/test/resources/csv_connector_with_schema"
  schema = "partition2 STRING, clustering1 STRING, partition1 INT, value LONG"
  inferSchema = "false"
  sep = ","
  header = "true"
  saveMode = "Append"
}
```

## JSON

### Options

| name    | description | optional | example  |
| ------  |  ---------- |  ------- |  ------- |
| path | directory of parquet files | false  |  |
| schema | Specifies the schema by using the input DDL-formatted string | true | `c1 STRING, c2 INT` |
| saveMode |  |  true, default `ErrorIfExists` | `Append` |
| filenamePattern | Regex of the names of file to be read. When `filenamePattern` is set, then this connector can only be used for reading data | true, default empty | `(file)(.*)(\\.csv)` |
| primitivesAsString  | infers all primitive values as a string type | true, default `false`  |  |
| prefersDecimal | infers all floating-point values as a decimal | true, default `false`   |  |
| allowComments  | ignores Java/C++ style comment in JSON records | true, default `false`|  | 
| allowUnquotedFieldNames | allows unquoted JSON field names | true, default `false`|  |
| allowSingleQuotes | allows single quotes in addition to double quotes | true, default `false`| |
| allowNumericLeadingZeros | allows leading zeros in numbers | true, default `false`| |
| multiLine  | parse one record, which may span multiple lines | true, default `false`| |
| encoding | allows to forcibly set one of standard basic or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. If the encoding is not specified and `multiLine` is set to `true`, it will be detected automatically. | true, default not set|  |
| lineSep | line separator | true, default default covers all `\r`, `\r\n` and `\n`|  |
| dateFormat  | sets the string that indicates a date format | true, default `yyyy-MM-dd`| |
| timestampFormat | sets the string that indicates a timestamp format  | true, default `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`| |
| dropFieldIfAllNull | whether to ignore column of all null values or empty array/struct during schema inference  | true, default `false` | |
| pathFormat | choose between REGEX path format (ex: `^s3:\/\/bucket\/\/col1=B\/col2=([A-C])$`) or WILDCARD path format (ex: `s3://bucket/*/*A*/internal-*.csv`) | true, default `REGEX`| `WILDCARD` |

For other options, please refer to [this doc](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html).

### Example

```
json {
  storage = "JSON"
  path = "src/test/resources/json_dir"
  saveMode = "Append"
}
```

## Parquet

### Options

| name    | description | optional | example  |
| ------  |  ---------- |  ------- |  ------- |
| path | directory of parquet files |   | |
| saveMode |  |  true, default `ErrorIfExists` | `Append` |
| filenamePattern | Regex of the names of file to be read. When `filenamePattern` is set, then this connector can only be used for reading data | true, default empty | `(file)(.*)(\\.csv)` |
| pathFormat | choose between REGEX path format (ex: `^s3:\/\/bucket\/\/col1=B\/col2=([A-C])$`) or WILDCARD path format (ex: `s3://bucket/*/*A*/internal-*.csv`) | true, default `REGEX`| `WILDCARD` |

### Example

```
parquet {
  storage = "PARQUET"
  path = "src/test/resources/parquet_dir" 
  saveMode = "Append"
}
```

The following connector will only read files that fit this regex in the given path
and ignore all the other files of the same location.

```
parquet {
  storage = "PARQUET"
  path = "src/test/resources/parquet_dir" 
  filenamePattern = "(file)(.*)(\\.csv)"  
  saveMode = "Append"
}
```

## Excel

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

For other options, please refer to [the documentation of spark-excel library](https://github.com/crealytics/spark-excel)

### Example

```
excel {
  storage = "EXCEL"
  path = "src/test/resources/test_excel.xlsx"
  useHeader = "true"
  treatEmptyValuesAsNulls = "true"
  inferSchema = "false"
  schema = "partition1 INT, partition2 STRING, clustering1 STRING, value LONG"
  saveMode = "Overwrite"
}
```

## DynamoDB

### Options

| name  | default |
| ------  |  ------- |
| region | <user_input>  |
| table | <user_input>  |
| saveMode | <user_input>  |

### Example

```
dynamodb {
  connector {
    storage = "DYNAMODB"
    region = "eu-west-1"
    table = "test-table"
    saveMode = "Overwrite"
  }
}
```

## Cassandra

### Options

| name  | default |
| ------  |  ------- |
| keyspace | <user_input>  |
| table | <user_input>  |
| partitionKeyColumns | `None` |
| clusteringKeyColumns | `None` |

### Example

```
cassandra {
  storage = "CASSANDRA"
  keyspace = "keyspace_name"
  table = "table_name"
  partitionKeyColumns = ["partition1", "partition2"]
  clusteringKeyColumns = ["clustering1"]
}
```

## JDBC

### Options

| name  | default |
| ------  |  ------- |
| url | <user_input>  |
| dbtable | <user_input>  |
| user | <user_input>  |
| password | <user_input>  |
| saveMode | `ErrorIfExists`  |
| driver | <user_input>  |


For other options, please refer to [the Spark documentation](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

### Example

```
jdbcExample {
  storage = "JDBC"
  url = "jdbc:postgresql://127.0.0.1:5432/databasename"
  dbtable = "tablename"
  saveMode = "Overwrite"
  user = "postgres"
  password = "postgres"
  driver = "org.postgresql.Driver"
}
```

