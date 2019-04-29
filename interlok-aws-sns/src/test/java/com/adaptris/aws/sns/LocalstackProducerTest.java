package com.adaptris.aws.sns;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.commons.lang3.BooleanUtils;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.interlok.config.DataInputParameter;

// A new local stack instance; we're going publish an SNS message.
// Note that there must be content to the message otherwise you get a python stack trace in localstack.
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

  }

  @Test
  public void test_01_TestPublish() throws Exception {
    if (areTestsEnabled()) {
      PublishToTopic producer = new PublishToTopic(new ConfiguredProduceDestination(config.getProperty(SNS_TOPIC)));
      AmazonSNSConnection connection = buildConnection();
      StandaloneProducer sp = new StandaloneProducer(connection, producer);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);
      
      ServiceCase.execute(sp, msg);
      assertTrue(msg.headersContainsKey(PublishToTopic.SNS_MSG_ID_KEY));
      assertNotNull(msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
      System.err.println("Published MessageID = " + msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
    } else {
      System.err.println("localstack disabled; not executing test_01_TestPublish");
    }
  }

  
  protected static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(config.getProperty(TESTS_ENABLED, "false"));
  }

  protected AmazonSNSConnection buildConnection() {
    String serviceEndpoint = config.getProperty(SNS_URL);
    String signingRegion = config.getProperty(SNS_SIGNING_REGION);
    AmazonSNSConnection connection = new AmazonSNSConnection(new AWSKeysAuthentication("TEST", "TEST"), null)
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
    return connection;
  }
}
