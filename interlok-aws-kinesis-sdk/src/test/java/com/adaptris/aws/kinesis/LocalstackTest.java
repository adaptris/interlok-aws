package com.adaptris.aws.kinesis;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackTest {

  private static final String TESTS_ENABLED = "localstack.tests.enabled";
  private static final String KINESIS_SIGNING_REGION = "localstack.kinesis.signingRegion";
  private static final String KINESIS_URL = "localstack.kinesis.url";
  private static final String KINESIS_STREAM = "localstack.kinesis.stream";
  private static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  private static final String MSG_CONTENTS = "hello world";

  private static final long MAX_WAIT = 65000;

  private static final Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  private transient AWSKinesisSDKConnection connection;

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
    System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
    this.connection = connection();
    LifecycleHelper.initAndStart(connection);
  }

  @After
  public void tearDown() throws Exception {
    LifecycleHelper.stopAndClose(connection);
    System.clearProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY);
  }

  @Test
  public void test_01_PutRecord() throws Exception {
    KinesisSDKStreamProducer producer = new KinesisSDKStreamProducer()
      .withStream(getProperty(KINESIS_STREAM))
      .withPartitionKey("key");

    AWSKinesisSDKConnection conn = connection();
    StandaloneProducer sp = new StandaloneProducer(conn, producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);

    ExampleServiceCase.execute(sp, msg);
  }

  private static String getProperty(String key) {
    return config.getProperty(key);
  }

  private static String getProperty(String key, String defaultValue) {
    return config.getProperty(key, defaultValue);
  }

  private static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(getProperty(TESTS_ENABLED, "false"));
  }

  private AWSKinesisSDKConnection connection() {
    String serviceEndpoint = getProperty(KINESIS_URL);
    String signingRegion = getProperty(KINESIS_SIGNING_REGION);
    AWSKinesisSDKConnection connection = new AWSKinesisSDKConnection()
      .withCredentialsProviderBuilder(
        new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("TEST", "TEST")))
      .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
    return connection;
  }

  private AmazonKinesis kinesisClient() throws Exception {
    return connection.kinesisClient();
  }
}
