package com.adaptris.aws2;

import com.amazonaws.ClientConfigurationFactory;
import com.amazonaws.client.builder.AwsClientBuilder;

public class MockAwsClientBuilder extends AwsClientBuilder {

  public MockAwsClientBuilder() {
    super(new ClientConfigurationFactory());
  }

  @Override
  public Object build() {
    throw new UnsupportedOperationException();
  }

}
