package com.adaptris.aws;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config aws-static-credentials-builder
 */
@XStreamAlias("aws-static-credentials-builder")
@ComponentProfile(summary = "Create a static set of credentials")
public class StaticCredentialsBuilder implements AWSCredentialsProviderBuilder {

  @NotNull
  @Valid
  @InputFieldDefault(value = "default-aws-authentication")
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

  public AWSAuthentication getAuthentication() {
    return authentication;
  }

  public void setAuthentication(AWSAuthentication a) {
    this.authentication = Args.notNull(a, "authentication");
  }

  public StaticCredentialsBuilder withAuthentication(AWSAuthentication a) {
    setAuthentication(a);
    return this;
  }
}
