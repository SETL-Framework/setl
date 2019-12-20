# SETL
SETL (Spark ETL, pronounced "settle") is an open source ETL framework for Apache Spark.

## Use

**The framework will be available in Maven Central Repository very soon!** For instance, run `mvn clear install -DskipTests` on local to install the package.

### Create a new project
You can start working by cloning [this template project](https://github.com/qxzzxq/setl-template).

### Use in an existing project
```xml
<dependency>
  <groupId>com.jcdecaux.setl</groupId>
  <artifactId>setl_2.11</artifactId>
  <version>0.4.0</version>
</dependency>
```

Make sure that you have already added **nexus.jcdecaux.com** into the project repositories. 
Otherwise, add to your `pom.xml`
```xml
<repositories>
  <repository>
    <id>nexus-datacorp-snapshots</id>
    <url>http://nexus.jcdecaux.com/repository/snapshots-DATACORP/</url>
  </repository>
  <repository>
    <id>nexus-datacorp-releases</id>
    <url>http://nexus.jcdecaux.com/repository/releases-DATACORP/</url>
  </repository>
</repositories>
```

## Documentation
[Check our wiki](https://github.com/JCDecaux/setl/wiki)

## Development
All the modifications in the master branche will be deployed to the snapshot repository.

All the tags starting with `v` will be deployed to the release repository.

