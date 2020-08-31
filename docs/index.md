![logo](img/logo_setl.png)
-----------
If you’re a **data scientist** or **data engineer**, this might sound familiar while working on **ETL** projects: 

- Switching between multiple projects is a hassle 
- Debugging others’ code is a nightmare
- Spending a lot of time solving non-business-related issues 

**SETL** (Spark ETL, pronounced "settle") is a Scala framework that helps you structure your Spark ETL projects, modularize your data transformation logic and speed up your development.

## Table of contents

- [Quick start](Quick-Start)
- [Setl](Setl)
- Data Access Layer
  - [Connector](Connector)
    - [FileConnector](Connector#fileconnector)
    - [DBConnector](Connector#dbconnector)
    - [StructuredStreamingConnector](Structured-Streaming-Connector)
    - [Use your own connector](CustomConnector)
  - [Repository](Repository)
  - [SparkRepositoryAdapter](SparkRepositoryAdapter)
  - [ConnectorBuilder](ConnectorBuilder)
  - [SparkRepositoryBuilder](SparkRepositoryBuilder)
- Data Transformation API
  - [Transformer](Transformer)
  - [Factory](Factory)
- Workflow Management
  - [Stage](Stage)
  - [Pipeline](Pipeline)
  - [Pipeline execution optimization (preliminary feature)](PipelineOptimizer)
- Utilities
  - [Annotations](Annotations)
  - [SparkSession builder](SparkSessionBuilder)
  - [ConfigLoader](ConfigLoader)
  - [DateUtils](DateUtils)
  - [Condition](Condition)
- Developper
  - [StructAnalyser](StructAnalyser)
  - [SchemaConverter](SchemaConverter)
  - [PipelineInspector](PipelineInspector)
  - [PipelineOptimizer](PipelineOptimizer)
  - [DeliverableDispatcher](DeliverableDispatcher)
  - [Read cache strategy](SparkRepository-caching)
  - [Logging](Logging)




