package com.adaptris.aws2;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;

// Not entirely sure this can be functional, since it is generic...
@FunctionalInterface
public interface EndpointBuilder {
  <T extends AwsClientBuilder<?, ?>> T rebuild(T builder);
}
