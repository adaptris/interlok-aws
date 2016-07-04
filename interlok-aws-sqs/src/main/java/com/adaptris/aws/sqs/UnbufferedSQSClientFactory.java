package com.adaptris.aws.sqs;

import java.util.concurrent.Executors;

import com.amazonaws.ClientConfiguration;
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
  public AmazonSQSAsync createClient(AWSCredentials creds, ClientConfiguration conf) {
    if(creds == null) {
      return new AmazonSQSAsyncClient(conf);
    } else {
      return new AmazonSQSAsyncClient(creds, conf, Executors.newFixedThreadPool(conf.getMaxConnections()));
    }
  }
  
}
