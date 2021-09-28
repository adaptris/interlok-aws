/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws2;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.util.KeyValuePairSet;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import javax.validation.Valid;

public abstract class AWSConnection extends AdaptrisConnectionImp
    implements AWSCredentialsProviderBuilder.BuilderConfig {

  /**
   * Set the region for the client.
   *
   * <p>
   * If the region is not specified, then {@link DefaultAwsRegionProviderChain} is used to determine
   * the region. You can always specify a region using the standard system property {@code aws2.region}
   * or via environment variables.
   * </p>
   *
   */
  @Getter
  @Setter
  private String region;

  /**
   * How to provide Credentials for AWS.
   * <p>
   * If not specified, then a static credentials provider with a default provider chain will be used.
   * </p>
   */
  @Getter
  @Setter
  @Valid
  @InputFieldDefault(value = "aws2-static-credentials-builder with default credentials")
  private AwsCredentialsProvider credentials;

  /**
   * Any specific client configuration.
   *
   */
  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private KeyValuePairSet clientConfiguration;

  /**
   * The Retry policy.
   *
   */
  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private RetryPolicyFactory retryPolicy;

  /**
   * The custom endpoint for this connection.
   * <p>
   * Generally speaking, you don't need to configure this; use {@link #setRegion(String)} instead.
   * This is only required if you are planning to use a non-standard service endpoint such as
   * <a href="https://github.com/localstack/localstack">localstack</a> to provide AWS services.
   * Explicitly configuring this means that your {@link #setRegion(String)} will have no effect (i.e.
   * {@code AwsClientBuilder#setRegion(String)} will never be invoked.
   * </p>
   * *
   */
  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private CustomEndpoint customEndpoint;

  public AwsCredentialsProvider credentialsProvider() {
    return AWSCredentialsProviderBuilder.defaultIfNull(getCredentials());
  }

  @Override
  public KeyValuePairSet clientConfiguration() {
    return ObjectUtils.defaultIfNull(getClientConfiguration(), new KeyValuePairSet());
  }

  @Override
  public RetryPolicyFactory retryPolicy() {
    return ObjectUtils.defaultIfNull(getRetryPolicy(), new DefaultRetryPolicyFactory());
  }

  public <T extends AWSConnection> T withCustomEndpoint(CustomEndpoint endpoint) {
    setCustomEndpoint(endpoint);
    return (T) this;
  }

  public <T extends AWSConnection> T withCredentialsProviderBuilder(AwsCredentialsProvider builder) {
    setCredentials(builder);
    return (T) this;
  }

  public <T extends AWSConnection> T withClientConfiguration(KeyValuePairSet cfg) {
    setClientConfiguration(cfg);
    return (T) this;
  }

  public <T extends AWSConnection> T withRetryPolicy(RetryPolicyFactory f) {
    setRetryPolicy(f);
    return (T) this;
  }

  public <T extends AWSConnection> T withRegion(String s) {
    setRegion(s);
    return (T) this;
  }

  /** Returns something that can configure a normal AWS builder with a custom endpoint or a region...
   *
   */
  @Override
  public EndpointBuilder endpointBuilder() {
    return getCustomEndpoint() != null && getCustomEndpoint().isConfigured() ? getCustomEndpoint()
        : new RegionEndpoint(getRegion());
  }

}
