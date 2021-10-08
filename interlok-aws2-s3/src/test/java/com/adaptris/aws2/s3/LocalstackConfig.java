package com.adaptris.aws2.s3;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.util.PropertyHelper;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Properties;

public class LocalstackConfig {

  public static final String MY_TAG_TEXT = "text";
  public static final String MY_TAG = "MyTag";
  public static final String TESTS_ENABLED = "localstack.tests.enabled";
  public static final String S3_SIGNING_REGION = "localstack.s3.signingRegion";
  public static final String S3_URL = "localstack.s3.url";
  public static final String S3_UPLOAD_FILENAME = "localstack.s3.upload.filename";
  public static final String S3_COPY_TO_FILENAME = "localstack.s3.copy.filename";
  public static final String S3_BUCKETNAME = "localstack.s3.bucketname";
  public static final String S3_FILTER_SUFFIX = "localstack.s3.ls.filterSuffix";
  public static final String S3_FILTER_REGEXP = "localstack.s3.ls.filterRegexp";
  public static final String S3_RETRY_PREFIX = "localstack.s3.retry.prefix";
  public static final String S3_RETRY_BUCKET_NAME = "localstack.s3.retry.bucketname";

  public static final String EXTENDED_COPY_ENABLED = "localstack.s3.extended.copy.tests";

  public static final String PROPERTIES_RESOURCE = "unit-tests.properties";

  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  public static String getConfiguration(String s) {
    return getConfiguration().getProperty(s);
  }

  public static Properties getConfiguration() {
    return config;
  }

  public static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(getConfiguration().getProperty(TESTS_ENABLED, "false"));
  }

  public static boolean extendedCopyTests() {
    return BooleanUtils.toBoolean(getConfiguration().getProperty(EXTENDED_COPY_ENABLED, "false"));
  }

  public static S3Service build(S3Operation operation) {
    return new S3Service(createConnection(), operation);
  }

  public static AmazonS3Connection createConnection() {
    String serviceEndpoint = getConfiguration().getProperty(S3_URL);
    String signingRegion = getConfiguration().getProperty(S3_SIGNING_REGION);
    AmazonS3Connection connection = new AmazonS3Connection()
        .withCredentialsProviderBuilder(new StaticCredentialsBuilder()
                .withAuthentication(new AWSKeysAuthentication("TEST", "TEST")))
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint)
            .withSigningRegion(signingRegion));
    return connection;
  }
}
