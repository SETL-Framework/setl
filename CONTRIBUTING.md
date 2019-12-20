# Contributing to SETL

Thanks a looooot for taking time to contribute :+1:

## Bug report
When you are creating a bug report, please include as many details as possible. 
Fill out the required template, the information it asks for helps us resolve issues faster.

## Feature request
When you are creating an enhancement suggestion, please include as many details as possible. 
Fill in the template, including the steps that you imagine you would take if the feature you're requesting existed.

## Local development

### Unit tests
Run the following commands before running the unit tests:
```shell
export AWS_ACCESS_KEY_ID="fakeAccess"
export AWS_SECRET_ACCESS_KEY="fakeSecret"
export AWS_REGION="eu-west-1"
docker-compose -f ./dev/docker-compose.yml up
```

Then you can start the unit test with `mvn clean test`

## Styleguide

### Commit styleguide
Please refer to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0-beta.2/)

### Scala styleguide
Please refer to [Databricks Scala Guide](https://github.com/databricks/scala-style-guide)
