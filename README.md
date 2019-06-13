# DataCorp Spark SDK
This project provides a general-proposed framework for data transformation application.

## Use
### Maven
```xml
<!--JCDecaux Datacorp-->
<dependency>
  <groupId>com.jcdecaux.datacorp</groupId>
  <artifactId>dc-spark-sdk</artifactId>
  <version>0.2.5</version>
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


## Build and deployment
Maven is used as the dependency manager in this project.

### Build
```bash
mvn clean package -Pprovided
```

### Deploy
```bash
# SNAPSHOT
mvn -Pprovided clean deploy 

# RELEASE
mvn -Dchangelist= -Pprovided clean deploy 
```
