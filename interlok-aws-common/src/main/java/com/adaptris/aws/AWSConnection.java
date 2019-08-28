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

package com.adaptris.aws;

import javax.validation.Valid;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import lombok.Getter;
import lombok.Setter;

public abstract class AWSConnection extends AdaptrisConnectionImp {

  /**
   * Set the region for the client.
   * 
   * <p>
   * If the region is not specified, then {@link DefaultAwsRegionProviderChain} is used to determine
   * the region. You can always specify a region using the standard system property {@code aws.region}
   * or via environment variables.
   * </p>
   * 
   */
  @Getter
  @Setter
  private String region;

  @Valid
  @Deprecated
  @Removal(version = "3.11.0", message = "Use a AWSCredentialsProviderBuilder instead")
  @Getter
  @Setter
  private AWSAuthentication authentication;

  /**
   * How to provide Credentials for AWS.
   * <p>
   * If not specified, then a static credentials provider with a default provider chain will be used.
   * </p>
   */
  @Getter
  @Setter
  @Valid
  @InputFieldDefault(value = "aws-static-credentials-builder with default credentials")
  private AWSCredentialsProviderBuilder credentials;

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

  @SuppressWarnings("deprecation")
  public AWSCredentialsProviderBuilder credentialsProvider() {
    return AWSCredentialsProviderBuilder.providerWithWarning(LoggingHelper.friendlyName(this), getAuthentication(),
        getCredentials());
  }

  public KeyValuePairSet clientConfiguration() {
    return ObjectUtils.defaultIfNull(getClientConfiguration(), new KeyValuePairSet());
  }

  public RetryPolicyFactory retryPolicy() {
    return ObjectUtils.defaultIfNull(getRetryPolicy(), new DefaultRetryPolicyFactory());
  }
  
  public <T extends AWSConnection> T withCustomEndpoint(CustomEndpoint endpoint) {
    setCustomEndpoint(endpoint);
    return (T) this;
  }
  
  public <T extends AWSConnection> T withCredentialsProviderBuilder(AWSCredentialsProviderBuilder builder) {
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
  protected EndpointBuilder endpointBuilder(){
    return getCustomEndpoint() != null && getCustomEndpoint().isConfigured() ? getCustomEndpoint()
        : new RegionOnly();
  }
  


  protected class RegionOnly implements EndpointBuilder {

    @Override
    public <T extends AwsClientBuilder<?, ?>> T rebuild(T builder) {
      if (StringUtils.isNotBlank(getRegion())) {
        log.trace("Setting Region to {}", getRegion());
        builder.setRegion(getRegion());
      }
      return builder;
    }
    
  }
}
