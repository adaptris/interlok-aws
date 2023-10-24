package com.adaptris.aws2.kinesis;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

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

    System.setProperty("com.amazonaws.sdk.disableCbor"/*SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY*/, "true");
    this.connection = connection();
    LifecycleHelper.initAndStart(connection);
  }

  @After
  public void tearDown() throws Exception {
    LifecycleHelper.stopAndClose(connection);
    System.clearProperty("com.amazonaws.sdk.disableCbor"/*SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY*/);
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

    assertEquals(MSG_CONTENTS, getRecords(getProperty(KINESIS_STREAM)));
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
    return new AWSKinesisSDKConnection()
            .withCredentialsProviderBuilder(
                    new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("TEST", "TEST")))
      .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
  }

  private KinesisClient kinesisClient() {
    return connection.kinesisClient();
  }

  private String getRecords(String streamName) {
    KinesisClient client = kinesisClient();

    List<Shard> initialShardData = client.describeStream(DescribeStreamRequest.builder().streamName(streamName).build()).streamDescription().shards();

    List<String> initialShardIterators = initialShardData.stream().map(s ->
      client.getShardIterator(GetShardIteratorRequest.builder()
              .streamName(streamName)
              .shardId(s.shardId())
              .startingSequenceNumber(s.sequenceNumberRange().startingSequenceNumber())
              .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER).build()
      ).shardIterator()
    ).collect(Collectors.toList());

    String shardIterator = initialShardIterators.get(0);

    GetRecordsRequest.Builder getRecordsRequest = GetRecordsRequest.builder();
    getRecordsRequest.shardIterator(shardIterator);
    getRecordsRequest.limit(25);

    GetRecordsResponse recordResult = client.getRecords(getRecordsRequest.build());
    return new String (recordResult.records().get(0).data().asByteArray(), StandardCharsets.UTF_8);
  }
}
