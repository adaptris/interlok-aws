package com.adaptris.aws.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;

public interface SQSClientFactory {

  /**
   * Create a new Amazon SQS client.
   * 
   * @param creds The credentials to use. If null the default credentials mechanism for the Amazon AWS SDK will be used.
   * @param conf the ClientConfiguration to use.
   * @return
   */
  public AmazonSQSAsync createClient(AWSCredentials creds, ClientConfiguration conf);
  
}
