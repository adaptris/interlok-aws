package com.adaptris.aws;

import com.amazonaws.auth.AWSCredentialsProvider;

public interface AWSCredentialsProviderBuilder {

  AWSCredentialsProvider build() throws Exception;
}
