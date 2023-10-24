package com.adaptris.aws2.sns;

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
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

// A new local stack instance; we're going publish an SNS message.
// Note that there must be content to the message otherwise you get a python stack trace in
// localstack.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackProducerTest {

  private static final String TESTS_ENABLED = "localstack.tests.enabled";
  private static final String SNS_SIGNING_REGION = "localstack.sns.signingRegion";
  private static final String SNS_URL = "localstack.sns.url";
  private static final String SNS_TOPIC = "localstack.sns.topic";
  private static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  private static final String MSG_CONTENTS = "hello world";

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
  }


  @Test
  public void test_01_TestPublish() throws Exception {
    String topic = createTopicArn();
    PublishToTopic producer = new PublishToTopic().withTopicArn(topic);
    AmazonSNSConnection connection = buildConnection();
    StandaloneProducer sp = new StandaloneProducer(connection, producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);

    ExampleServiceCase.execute(sp, msg);
    assertTrue(msg.headersContainsKey(PublishToTopic.SNS_MSG_ID_KEY));
    assertNotNull(msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
    System.err
        .println("Published MessageID = " + msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
  }


  protected static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(config.getProperty(TESTS_ENABLED, "false"));
  }


  private String createTopicArn() throws Exception {
    AmazonSNSConnection connection = buildConnection();
    try {
      LifecycleHelper.initAndStart(connection);
      SnsClient client = connection.amazonClient();
      CreateTopicRequest createTopicRequest = CreateTopicRequest.builder().name(config.getProperty(SNS_TOPIC)).build();
      CreateTopicResponse createTopicResponse = client.createTopic(createTopicRequest);
      return createTopicResponse.topicArn();
    } finally {
      LifecycleHelper.stopAndClose(connection);
    }
  }

  protected AmazonSNSConnection buildConnection() {
    String serviceEndpoint = config.getProperty(SNS_URL);
    String signingRegion = config.getProperty(SNS_SIGNING_REGION);
    AmazonSNSConnection connection = new AmazonSNSConnection()
        .withCredentialsProviderBuilder(new StaticCredentialsBuilder()
            .withAuthentication(new AWSKeysAuthentication("TEST", "TEST")))
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint)
            .withSigningRegion(signingRegion));
    return connection;
  }
}
