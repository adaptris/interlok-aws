package com.adaptris.aws.kms;

import software.amazon.awssdk.services.kms.KmsClient;

public interface ClientWrapper<T extends KmsClient> {

  T awsClient() throws Exception;

  static void shutdownQuietly(KmsClient client) {
    if (client != null) {
      client.close();
    }
  }
}
