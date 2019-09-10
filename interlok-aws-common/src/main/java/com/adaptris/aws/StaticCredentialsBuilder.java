package com.adaptris.aws;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * A static set of credentials for AWS.
 * 
 * @config aws-static-credentials-builder
 */
@XStreamAlias("aws-static-credentials-builder")
@ComponentProfile(summary = "Create a static set of credentials", since = "3.9.1")
public class StaticCredentialsBuilder implements AWSCredentialsProviderBuilder {

  @NotNull
  @Valid
  @InputFieldDefault(value = "default-aws-authentication")
  @NonNull
  @Getter
  @Setter
  private AWSAuthentication authentication;

  public StaticCredentialsBuilder() {
    setAuthentication(new DefaultAWSAuthentication());
  }

  @Override
  public AWSCredentialsProvider build() throws Exception {
    AWSCredentials credentials = getAuthentication().getAWSCredentials();
    if (credentials == null) {
      return DefaultAWSCredentialsProviderChain.getInstance();
    }
    return new AWSStaticCredentialsProvider(credentials);
  }

  public StaticCredentialsBuilder withAuthentication(AWSAuthentication a) {
    setAuthentication(a);
    return this;
  }
}
