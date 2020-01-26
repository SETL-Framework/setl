# Contributing to SETL

Thanks sooooo much for taking time to contribute :+1:

## Bug report

When you are creating a bug report, please include as many details as possible. 
Fill out the required template, the information it asks for helps us resolve issues faster.

## Feature request

When you are creating an enhancement suggestion, please include as many details as possible. 
Fill in the template, including the steps that you imagine you would take if the feature you're requesting existed.

## Development

### Quick guide
- Fork the project & clone locally.
- Create an upstream remote and sync your local copy before you branch.
- Branch for each separate piece of work.
- Push to the origin repository (the fork).
- Create a new Pull Request in GitHub.

### Build

Use pre-created profiles to change version.

```shell
# Build SNAPSHOT with Scala 2.11
mvn clean package -Psnapshot -Pscala_2.11

# Build RELEASE with Scala 2.11
mvn clean package -Prelease -Pscala_2.11

# Build SNAPSHOT with Scala 2.12
./dev/change-scala-version.sh 2.12
mvn clean package -Psnapshot -Pscala_2.12

# Build RELEASE with Scala 2.12
./dev/change-scala-version.sh 2.12
mvn clean package -Prelease -Pscala_2.12
```

### Unit tests

We use docker to provide services for the unit test. Run the following command before the unit test:
```shell
docker-compose -f ./dev/docker-compose.yml up
```

To start the test with cli:
```shell
export SCALA_VER=2.11
./dev/test.sh
```

## Styleguide

### Commit styleguide

Please refer to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0-beta.2/)

### Scala styleguide

Please refer to [Databricks Scala Guide](https://github.com/databricks/scala-style-guide)
