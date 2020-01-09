# SETL

![](https://codebuild.eu-west-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiWEJ3TXdNTUVsaUgveFE0V01xM1AvMkZaamVId1JFRGY0MDRuVWN1dEdmNkFKaFpmdmNlZXlTNFpGWjlXODFVQUNXdEIvc2tacXZuc3MySGpFU2gwVnk4PSIsIml2UGFyYW1ldGVyU3BlYyI6ImVqTmtldXdCWVlyd2JnMW0iLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)


SETL (Spark ETL, pronounced "settle") is an open-source Spark ETL framework that helps developers to structure ETL projects, modularize data transformation components and speed up the development.

## Use

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

To use the SNAPSHOT version, add Sonatype snapshot repository to your `pom.xml`
```xml
<repositories>
  <repository>
    <id>ossrh-snapshots</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.jcdecaux.setl</groupId>
    <artifactId>setl_2.11</artifactId>
    <version>0.4.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

## Documentation
[Check our wiki](https://github.com/JCDecaux/setl/wiki)

## Contributing to SETL
[Check our contributing guide](https://github.com/JCDecaux/setl/blob/master/CONTRIBUTING.md)

