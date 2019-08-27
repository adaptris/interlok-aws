package com.adaptris.aws;

import com.amazonaws.auth.AWSCredentialsProvider;

@FunctionalInterface
public interface AWSCredentialsProviderBuilder {

  AWSCredentialsProvider build() throws Exception;
}
