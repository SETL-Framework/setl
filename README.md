# SETL
SETL (Spark ETL, pronounced "settle") is an open-source Spark ETL framework that helps developers to structure ETL projects, modularize data transformation components and speed up the development.

## Use

**Currently, only the SNAPSHOT version is available. The release version will be available very soon in the Central Repository** 

To use the SNAPSHOT version: 

Add Sonatype snapshot repository to your `pom.xml`
```xml
<repositories>
  <repository>
    <id>ossrh-snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
  </repository>
</repositories>
```

### Create a new project
You can start working by cloning [this template project](https://github.com/qxzzxq/setl-template).

### Use in an existing project
```xml
<dependency>
  <groupId>com.jcdecaux.setl</groupId>
  <artifactId>setl_2.11</artifactId>
  <version>0.4.0-SNAPSHOT</version>
</dependency>
```

## Documentation
[Check our wiki](https://github.com/JCDecaux/setl/wiki)

## Contributing to SETL
[Check our contributing guide](https://github.com/JCDecaux/setl/blob/master/CONTRIBUTING.md)

