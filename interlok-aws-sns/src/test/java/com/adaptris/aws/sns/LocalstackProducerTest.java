package com.adaptris.aws.sns;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Properties;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;

// A new local stack instance; we're going publish an SNS message.
// Note that there must be content to the message otherwise you get a python stack trace in
// localstack.
@TestMethodOrder(MethodOrderer.MethodName.class)
public class LocalstackProducerTest {

  private static final String TESTS_ENABLED = "localstack.tests.enabled";
  private static final String SNS_SIGNING_REGION = "localstack.sns.signingRegion";
  private static final String SNS_URL = "localstack.sns.url";
  private static final String SNS_TOPIC = "localstack.sns.topic";
  private static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  private static final String MSG_CONTENTS = "hello world";

  @BeforeEach
  public void setUp() throws Exception {
    assumeTrue(areTestsEnabled());
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
      AmazonSNSClient client = connection.amazonClient();
      CreateTopicRequest createTopicRequest = new CreateTopicRequest(config.getProperty(SNS_TOPIC));
      CreateTopicResult createTopicResponse = client.createTopic(createTopicRequest);
      return createTopicResponse.getTopicArn();
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
