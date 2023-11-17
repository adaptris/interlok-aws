package com.adaptris.aws2;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;


/**
 * A static set of credentials for AWS.
 *
 * @config aws2-static-credentials-builder
 * @since 4.3.0
 */
@XStreamAlias("aws2-static-credentials-builder")
@ComponentProfile(summary = "Create a static set of credentials", since = "4.3.0")
public class StaticCredentialsBuilder implements AWSCredentialsProviderBuilder {

  @NotNull
  @Valid
  @InputFieldDefault(value = "default-aws2-authentication")
  @NonNull
  @Getter
  @Setter
  private AWSAuthentication authentication;

  public StaticCredentialsBuilder() {
    setAuthentication(new DefaultAWSAuthentication());
  }

  @Override
  public AwsCredentialsProvider build() throws Exception {
    AwsCredentials credentials = getAuthentication().getAWSCredentials();
    if (credentials == null) {
      return DefaultCredentialsProvider.create();
    }
    return StaticCredentialsProvider.create(credentials);
  }

  public StaticCredentialsBuilder withAuthentication(AWSAuthentication a) {
    setAuthentication(a);
    return this;
  }
}
