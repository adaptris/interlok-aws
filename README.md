# interlok-aws
[![GitHub tag](https://img.shields.io/github/tag/adaptris/interlok-aws.svg)](https://github.com/adaptris/interlok-aws/tags) ![license](https://img.shields.io/github/license/adaptris/interlok-aws.svg) [![codecov](https://codecov.io/gh/adaptris/interlok-aws/branch/develop/graph/badge.svg)](https://codecov.io/gh/adaptris/interlok-aws) [![Known Vulnerabilities](https://snyk.io/test/github/adaptris/interlok-aws/badge.svg?targetFile=interlok-aws-sqs%2Fbuild.gradle)](https://snyk.io/test/github/adaptris/interlok-aws?targetFile=interlok-aws-sqs%2Fbuild.gradle) [![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/adaptris/interlok-aws.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/adaptris/interlok-aws/context:java)

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
      - "4575:4566"
      #SQS - http://localhost:4576
      - "4576:4566"
      #S3 - http://localhost:4572
      - "4572:4566"
      #KMS - http://localhost:4599
      - "4599:4566"
```

## Enabling the localstack tests

For each sub module that you want to enable tests for; create a file build.properties with a value enables the localstack tests. It defaults to false which disables the tests and prints something to that effect on System.err

```
junit.localstack.tests.enabled=true
```

If you need to override various ports and other things, then check the contents of unit-tests.properties.template in the appropriate `src/test/resources` directory.

# AWS Kinesis

## AWS Kinesis + Windows

The AWS Kinesis component uses the simplified AWS kinesis producer; under the covers this spawns a native executable. Since it's native executables you may need to inspect `amazon-kinesis-producer.jar` manually to check that your OS is represented. We have found that sometimes the _Windows_ binary is missing); since the underlying system is opaque to us, this well be intentional on Amazon's part. A listing of recent jar files reveals :

version | Windows | MacOS | Linux |
-------|------------| ------| -----|
0.13 | __No__ | Yes | Yes |
0.13.1| Yes | Yes | Yes |
0.14|  __No__ | Yes | Yes |
0.14.1| __No__ | Yes | Yes |

That table was compiled by just eyeballing the jar file :

```
$ jar -tvf build/distribution/lib/amazon-kinesis-producer.jar | grep native
     0 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/
     0 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/osx/
     0 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/linux/
     0 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/windows/
11797944 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/osx/kinesis_producer
65780579 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/linux/kinesis_producer
3792384 Wed Jul 31 22:20:56 BST 2019 amazon-kinesis-producer-native-binaries/windows/kinesis_producer.exe
```

If you need to pin to a specific version then we suggest you something like this when managing your dependencies (gradle)

```
  interlokRuntime ("com.adaptris:interlok-aws-kinesis:3.11-SNAPSHOT") {
    changing=true
    exclude group: "com.amazonaws", module: "amazon-kinesis-producer"
  }
  // 0.13 and 0.14 don't contain the windows binaries...
  interlokRuntime ("com.amazonaws:amazon-kinesis-producer:0.13.1")
```

## AWS Kinesis + docker

Note that the native binary on Linux is compiled against `glibc` which means that you may get a stacktrace that looks like this (generally on alpine based images, since they use _muslc_ as the standard C library rather than _glibc_). It's probably easier at this point to choose a docker image that isn't alpine based.

```
com.amazonaws.services.kinesis.producer.IrrecoverableError: Error starting child process
        at com.amazonaws.services.kinesis.producer.Daemon.fatalError(Daemon.java:525) [amazon-kinesis-producer-0.12.8.jar:na]
        at com.amazonaws.services.kinesis.producer.Daemon.startChildProcess(Daemon.java:456) [amazon-kinesis-producer-0.12.8.jar:na]
        at com.amazonaws.services.kinesis.producer.Daemon.access$100(Daemon.java:63) [amazon-kinesis-producer-0.12.8.jar:na]
        at com.amazonaws.services.kinesis.producer.Daemon$1.run(Daemon.java:133) [amazon-kinesis-producer-0.12.8.jar:na]
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [na:1.8.0_151]
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [na:1.8.0_151]
        at java.lang.Thread.run(Thread.java:748) [na:1.8.0_151]
Caused by: java.io.IOException: Cannot run program "/tmp/amazon-kinesis-producer-native-binaries/kinesis_producer_0529847a6647765630c02beeaf6a22c24858f873": error=2, No such file or directory
        at java.lang.ProcessBuilder.start(ProcessBuilder.java:1048) ~[na:1.8.0_151]
        at com.amazonaws.services.kinesis.producer.Daemon.startChildProcess(Daemon.java:454) [amazon-kinesis-producer-0.12.8.jar:na]
        ... 5 common frames omitted
Caused by: java.io.IOException: error=2, No such file or directory
```
