package com.adaptris.aws;

import com.adaptris.annotation.ComponentProfile;
import com.amazonaws.auth.AWSCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Use the default authentication logic of the AWS SDK (IAM Roles, System Properties, Environment variables, etc
 */
@XStreamAlias("aws-default-authentication")
@ComponentProfile(summary="Use the default authentication logic of the AWS SDK (IAM Roles, System Properties, Environment variables, etc")
public class DefaultAWSAuthentication implements AWSAuthentication {

  @Override
  public AWSCredentials getAWSCredentials() {
    return null;
  }

}
