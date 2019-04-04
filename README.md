# DataCorp Spark SDK
This project provides a general-proposed framework for data transformation application.

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
