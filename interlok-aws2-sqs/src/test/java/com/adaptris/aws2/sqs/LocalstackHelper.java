package com.adaptris.aws2.sqs;

import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.PropertyHelper;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

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
    AmazonSQSConnection connection = new AmazonSQSConnection()
        .withCredentialsProviderBuilder(StaticCredentialsProvider.create(AwsBasicCredentials.create("TEST", "TEST")))
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
    return connection;
  }
  
  public SqsClient getSyncClient() throws Exception {
    return connection.getSyncClient();
  }

  public SqsAsyncClient getASyncClient() throws Exception {
    return connection.getASyncClient();
  }
  
  public String toQueueURL(String queueName) throws Exception {
    SqsClient sqs = getSyncClient();
    GetQueueUrlResponse queueURL = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
    return queueURL.queueUrl();
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
    SqsClient sqs = getSyncClient();
    GetQueueAttributesResponse result = sqs.getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(queueURL).attributeNames(Arrays.asList(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)).build());
    return Integer.valueOf(result.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
  }
 
}
