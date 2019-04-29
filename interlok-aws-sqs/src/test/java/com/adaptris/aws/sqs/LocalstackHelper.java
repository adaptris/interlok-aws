package com.adaptris.aws.sqs;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.BooleanUtils;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.sqs.jms.AdvancedSQSImplementation;
import com.adaptris.core.jms.JmsConnection;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.PropertyHelper;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;

// Since we have both an polling consumer and a JMS one, abstract the config to a helper (!)
public class LocalstackHelper {
  public static final String TESTS_ENABLED = "localstack.tests.enabled";
  public static final String SQS_SIGNING_REGION = "localstack.sqs.signingRegion";
  public static final String SQS_URL = "localstack.sqs.url";
  public static final String SQS_QUEUE = "localstack.sqs.queue";
  public static final String SQS_JMS_QUEUE = "localstack.sqs.jms.queue";
  public static final String PROPERTIES_RESOURCE = "unit-tests.properties";

  private static final long MAX_WAIT = 65000;
  
  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  private transient AmazonSQSConnection connection;
  
  public LocalstackHelper() throws Exception {
    connection = LifecycleHelper.initAndStart(createConnection());
  }
  
  public void shutdown() {
    LifecycleHelper.stopAndClose(connection);
  }
  
  public static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(getProperty(TESTS_ENABLED, "false"));
  }
  
  public static String getProperty(String key) {
    return config.getProperty(key);
  }
  
  public static String getProperty(String key, String defaultValue) {
    return config.getProperty(key, defaultValue);
  }
  
  public AmazonSQSConnection createConnection() {
    String serviceEndpoint = getProperty(SQS_URL);
    String signingRegion = getProperty(SQS_SIGNING_REGION);
    AmazonSQSConnection connection = new AmazonSQSConnection(new AWSKeysAuthentication("TEST", "TEST"), null)
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
    return connection;
  }
  
  public JmsConnection createJmsConnection() {
    String serviceEndpoint = getProperty(SQS_URL);
    String signingRegion = getProperty(SQS_SIGNING_REGION);
    BufferedSQSClientFactory clientFactory = new BufferedSQSClientFactory();
    clientFactory.setLongPollWaitTimeoutSeconds(5);
    AdvancedSQSImplementation jmsImpl = new AdvancedSQSImplementation()
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion))
        .withClientFactory(clientFactory)
        .withAuthentication(new AWSKeysAuthentication("TEST", "TEST"));
    JmsConnection jmsC = new JmsConnection(jmsImpl);
    return jmsC;
  }
  
  public AmazonSQS getSyncClient() throws Exception {
    return connection.getSyncClient();
  }

  public AmazonSQSAsync getASyncClient() throws Exception {
    return connection.getASyncClient();
  }
  
  
  public String toQueueURL(String queueName) throws Exception {
    AmazonSQS sqs = getSyncClient();
    GetQueueUrlResult queueURL = sqs.getQueueUrl(queueName);
    return queueURL.getQueueUrl();
  }
  
  public void waitForMessagesToAppear(String queueName, int count) throws Exception{
    waitForMessagesToAppear(queueName, count, MAX_WAIT);
  }
  
  public void waitForMessagesToAppear(String queueName, int count, long maxWait) throws Exception{
    String queueURL = toQueueURL(queueName);
    long totalWaitTime = 0;

    while (messagesOnQueue(queueURL) < count &&  totalWaitTime < maxWait) {
      long wait = (long) ThreadLocalRandom.current().nextInt(100) + 1;
      LifecycleHelper.waitQuietly(wait);
      totalWaitTime += wait;
    }
  }
  
  public int messagesOnQueue(String queueURL) throws Exception {
    AmazonSQS sqs = getSyncClient();
    GetQueueAttributesResult result = sqs.getQueueAttributes(
        new GetQueueAttributesRequest(queueURL).withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
    return Integer.valueOf(result.getAttributes().get(QueueAttributeName.ApproximateNumberOfMessages.toString()));
  }
 
}
