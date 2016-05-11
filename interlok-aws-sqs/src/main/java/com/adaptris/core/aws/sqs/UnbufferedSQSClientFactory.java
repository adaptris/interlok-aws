package com.adaptris.core.aws.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
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

  @Override
  public AmazonSQSAsync createClient(AWSCredentials creds) {
    if(creds == null) {
      return new AmazonSQSAsyncClient();
    } else {
      return new AmazonSQSAsyncClient(creds);
    }
  }
  
}
