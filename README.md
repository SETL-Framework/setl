# DataCorp Spark Framework
This project provides a general-proposed framework for data transformation application.

## Use
### Maven
```xml
<!--JCDecaux Datacorp-->
<dependency>
  <groupId>com.jcdecaux.datacorp</groupId>
  <artifactId>dc-spark-sdk_2.4</artifactId>
  <version>0.3.2</version>
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
[Click here](https://git.jcdecaux.com/DataCorp/dc-spark-sdk/wikis/home)
