package com.adaptris.core.aws.sqs;

import com.amazonaws.auth.AWSCredentials;

public class DefaultAWSAuthentication implements AWSAuthentication {

  public DefaultAWSAuthentication() {
  }

  @Override
  public AWSCredentials getAWSCredentials() {
    return null;
  }

}
