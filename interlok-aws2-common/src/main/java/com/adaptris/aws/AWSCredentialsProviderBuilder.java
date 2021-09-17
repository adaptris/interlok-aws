package com.adaptris.aws;

import com.adaptris.util.KeyValuePairSet;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@FunctionalInterface
public interface AWSCredentialsProviderBuilder {

  AwsCredentialsProvider build() throws Exception;

  default AwsCredentialsProvider build(BuilderConfig conf) throws Exception {
    return build();
  }

  static AWSCredentialsProviderBuilder defaultIfNull(AWSCredentialsProviderBuilder builder) {
    return ObjectUtils.defaultIfNull(builder, new StaticCredentialsBuilder().withAuthentication(new DefaultAWSAuthentication()));
  }

  interface BuilderConfig {
    KeyValuePairSet clientConfiguration();

    EndpointBuilder endpointBuilder();

    RetryPolicyFactory retryPolicy();
  }
}
