package com.adaptris.aws2;

import com.adaptris.util.KeyValuePairSet;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@FunctionalInterface
public interface AWSCredentialsProviderBuilder {

  AwsCredentialsProvider build() throws Exception;

  default AwsCredentialsProvider build(BuilderConfig conf) throws Exception {
    return build();
  }

  static AwsCredentialsProvider defaultIfNull(AwsCredentialsProvider provider) {
    return ObjectUtils.defaultIfNull(provider, StaticCredentialsProvider.create(new DefaultAWSAuthentication().getAWSCredentials()));
  }

  interface BuilderConfig {
    KeyValuePairSet clientConfiguration();

    EndpointBuilder endpointBuilder();

    RetryPolicyFactory retryPolicy();
  }
}
