package com.adaptris.aws.kms;

import com.amazonaws.AmazonWebServiceClient;

public interface ClientWrapper<T extends AmazonWebServiceClient> {

  T awsClient() throws Exception;

  static void shutdownQuietly(AmazonWebServiceClient client) {
    if (client != null) {
      client.shutdown();
    }
  }
}
