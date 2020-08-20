## Definition

**SchemaConverter** can:
- Convert a Dataset[A] to a DataFrame with the metadata of class **A** (extracted by **StructAnalyser**)
- Convert a DataFrame to a Dataset[A]

For each of the three annotations: ColumnName, CompoundKey and Compress, SchemaConverter will
- rename the column
- create/drop the compound key column(s)
- compress/decompress the column(s) having Compress annotation.

## Demo

### Dataset to DataFrame
```scala
val ds: Dataset[MyClass] = ...
SchemaConverter.toDF(ds)
```

### DataFrame to Dataset
```scala
val df: DataFrame = ...
SchemaConverter.fromDF[MyClass](df)
```