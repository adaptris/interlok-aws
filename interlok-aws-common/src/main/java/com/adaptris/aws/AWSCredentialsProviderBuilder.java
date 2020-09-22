package com.adaptris.aws;

import org.apache.commons.lang3.ObjectUtils;
import com.amazonaws.auth.AWSCredentialsProvider;

@FunctionalInterface
public interface AWSCredentialsProviderBuilder {

  AWSCredentialsProvider build() throws Exception;

  static AWSCredentialsProviderBuilder defaultIfNull(AWSCredentialsProviderBuilder builder) {
    return ObjectUtils.defaultIfNull(builder,
        new StaticCredentialsBuilder().withAuthentication(new DefaultAWSAuthentication()));
  }
}
