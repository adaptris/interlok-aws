package com.adaptris.aws.kms;

import com.adaptris.interlok.util.Args;
import com.amazonaws.services.kms.AWSKMSClient;

public class MockKmsConnection extends AWSKMSConnection {

  private transient AWSKMSClient mock;

  public MockKmsConnection(AWSKMSClient mockClient) {
    mock = Args.notNull(mockClient, "mockClient");
  }

  @Override
  public AWSKMSClient awsClient() throws Exception {
    return mock;
  }
}
