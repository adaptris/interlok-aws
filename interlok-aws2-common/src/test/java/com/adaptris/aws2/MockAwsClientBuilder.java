package com.adaptris.aws2;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

public class MockAwsClientBuilder implements AwsClientBuilder
{
  public URI endpoint;
  public Region region;

  @Override
  public Object build() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AwsClientBuilder credentialsProvider(AwsCredentialsProvider credentialsProvider)
  {
    return this;
  }

  @Override
  public AwsClientBuilder region(Region region)
  {
    return this;
  }

  @Override
  public SdkClientBuilder overrideConfiguration(ClientOverrideConfiguration overrideConfiguration)
  {
    return this;
  }

  @Override
  public SdkClientBuilder endpointOverride(URI endpointOverride)
  {
    return this;
  }
}
