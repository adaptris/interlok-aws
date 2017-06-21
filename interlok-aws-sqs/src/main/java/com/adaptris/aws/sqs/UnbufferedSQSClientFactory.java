package com.adaptris.aws.sqs;

import static com.adaptris.aws.sqs.AwsHelper.formatRegion;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 
 * Unbuffered SQS Client Factory.
 * 
 * @config unbuffered-sqs-client-factory
 * @since 3.0.3
 */
@XStreamAlias("unbuffered-sqs-client-factory")
public class UnbufferedSQSClientFactory implements SQSClientFactory {

  public UnbufferedSQSClientFactory() {
  }

  @Override
  public AmazonSQSAsync createClient(AWSCredentials creds, ClientConfiguration conf, String region) {
    AmazonSQSAsyncClientBuilder builder = AmazonSQSAsyncClientBuilder.standard().withClientConfiguration(conf)
        .withRegion(formatRegion(region));
    if (creds != null) {
      builder.withCredentials(new AWSStaticCredentialsProvider(creds));
    }
    return builder.build();
  }
  
}
