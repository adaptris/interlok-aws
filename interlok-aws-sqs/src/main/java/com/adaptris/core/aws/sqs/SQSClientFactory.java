package com.adaptris.core.aws.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;

public interface SQSClientFactory {

  /**
   * Create a new Amazon SQS client.
   * 
   * @param creds The credentials to use. If null the default credentials mechanism for the Amazon AWS SDK will be used.
   * @return
   */
  public AmazonSQSAsync createClient(AWSCredentials creds);
  
}
