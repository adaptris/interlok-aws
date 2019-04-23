# interlok-aws 
[![GitHub tag](https://img.shields.io/github/tag/adaptris/interlok-aws.svg)](https://github.com/adaptris/interlok-aws/tags) ![license](https://img.shields.io/github/license/adaptris/interlok-aws.svg) [![Build Status](https://travis-ci.org/adaptris/interlok-aws.svg?branch=develop)](https://travis-ci.org/adaptris/interlok-aws) [![CircleCI](https://circleci.com/gh/adaptris/interlok-aws/tree/develop.svg?style=svg)](https://circleci.com/gh/adaptris/interlok-aws/tree/develop) [![codecov](https://codecov.io/gh/adaptris/interlok-aws/branch/develop/graph/badge.svg)](https://codecov.io/gh/adaptris/interlok-aws) [![Known Vulnerabilities](https://snyk.io/test/github/adaptris/interlok-aws/badge.svg?targetFile=interlok-aws-sqs%2Fbuild.gradle)](https://snyk.io/test/github/adaptris/interlok-aws?targetFile=interlok-aws-sqs%2Fbuild.gradle) [![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/adaptris/interlok-aws.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/adaptris/interlok-aws/context:java)

The suggested name was `musical-octo-waffle`

# Testing with localstack

If you don't do anything else, then all the testing (that gives you the [![codecov](https://codecov.io/gh/adaptris/interlok-aws/branch/develop/graph/badge.svg)](https://codecov.io/gh/adaptris/interlok-aws) score) is done via mocking to check behaviour. However, there is the ability to perform some limited integration tests using localstack as the AWS provider. The expectation is that localstack is spun up in a separately; if you have docker-compose then you can use `docker-compose up` with this configuration file.

```
version: '3.2'
services:
  localstack:
    image: localstack/localstack
    environment:
      - SERVICES=s3,sqs,sns
    ports:
      #Management - http://localhost:8025
      - "8025:8080"
      #SNS - http://localhost:4575
      - "4575:4575"
      #SQS - http://localhost:4576
      - "4576:4576"
      #S3 - http://localhost:4572
      - "4572:4572"
```

## Enabling the localstack tests

For each sub module that you want to enable tests for; create a file build.properties with a value enables the localstack tests. It defaults to false which disables the tests and prints something to that effect on System.err

```
junit.localstack.tests.enabled=true
```

If you need to override various ports and other things, then check the contents of unit-tests.properties.template in the appropriate `src/test/resources` directory.