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

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;

public abstract class AWSConnection extends AdaptrisConnectionImp {
  private String region;

  @Valid
  private AWSAuthentication authentication;

  @Valid
  @AdvancedConfig
  private KeyValuePairSet clientConfiguration;

  @Valid
  @AdvancedConfig
  private RetryPolicyFactory retryPolicy;

  public AWSAuthentication getAuthentication() {
    return authentication;
  }

  public AWSAuthentication authentication() {
    return getAuthentication() != null ? getAuthentication() : new DefaultAWSAuthentication();
  }

  /**
   * The authentication method to use
   * 
   * @param auth the authentication to use, defaults to {@code DefaultAWSAuthentication} if not specified.
   */
  public void setAuthentication(AWSAuthentication auth) {
    this.authentication = auth;
  }

  /**
   * @return the clientConfiguration
   */
  public KeyValuePairSet getClientConfiguration() {
    return clientConfiguration;
  }

  /**
   * @param b the clientConfiguration to set
   */
  public void setClientConfiguration(KeyValuePairSet b) {
    this.clientConfiguration = b;
  }

  public KeyValuePairSet clientConfiguration() {
    return getClientConfiguration() != null ? getClientConfiguration() : new KeyValuePairSet();
  }

  public RetryPolicyFactory getRetryPolicy() {
    return retryPolicy;
  }

  /**
   * Set the retry policy if required.
   * 
   * @param rp the retry policy, defaults to {@code DefaultRetryPolicyBuilder} if not specified.
   */
  public void setRetryPolicy(RetryPolicyFactory rp) {
    this.retryPolicy = rp;
  }

  public RetryPolicyFactory retryPolicy() {
    return getRetryPolicy() != null ? getRetryPolicy() : new DefaultRetryPolicyFactory();
  }

  public String getRegion() {
    return region;
  }

  /**
   * Set the region for the client.
   * 
   * <p>
   * If the region is not specified, then {@link DefaultAwsRegionProviderChain} is used to determine the region. You can always
   * specify a region using the standard system property {@code aws.region} or via environment variables.
   * </p>
   * 
   * @param s the region.
   */
  public void setRegion(String s) {
    this.region = s;
  }
}
