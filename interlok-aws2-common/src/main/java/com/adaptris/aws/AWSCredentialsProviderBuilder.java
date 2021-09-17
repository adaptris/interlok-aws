package com.adaptris.aws;

import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.auth.AWSCredentialsProvider;

@FunctionalInterface
public interface AWSCredentialsProviderBuilder {

  AWSCredentialsProvider build() throws Exception;

  default AWSCredentialsProvider build(BuilderConfig conf) throws Exception {
    return build();
  }

  static AWSCredentialsProviderBuilder defaultIfNull(AWSCredentialsProviderBuilder builder) {
    return ObjectUtils.defaultIfNull(builder,
        new StaticCredentialsBuilder().withAuthentication(new DefaultAWSAuthentication()));
  }

  interface BuilderConfig {
    KeyValuePairSet clientConfiguration();

    EndpointBuilder endpointBuilder();

    RetryPolicyFactory retryPolicy();
  }
}
