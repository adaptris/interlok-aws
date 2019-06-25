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

package com.adaptris.aws.sqs.jms;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.util.Args;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.EndpointBuilder;
import com.adaptris.aws.RetryPolicyFactory;
import com.adaptris.util.KeyValuePairSet;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.sqs.AmazonSQS;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * JMS VendorImplementation for Amazon SQS.
 * <p>
 * This VendorImplementation uses the Amazon SQS JMS compatibility layer. When using this class, do not use the AmazonSQS Producer
 * and Consumer classes. Use regular JMS consumers and producers instead.
 * </p>
 * <p>
 * The key from the <code>client-configuration-properties</code> element should match the name of the underlying ClientConfiguration
 * property. So if you wanted to control the user-agent you would configure
 * </p>
 * <pre>
 * {@code 
 *   <client-configuration-properties>
 *     <key-value-pair>
 *        <key>UserAgent</key>
 *        <value>My User Agent</value>
 *     </key-value-pair>
 *   </client-configuration-properties>
 * }
 * </pre>
 * 
 * @config advanced-amazon-sqs-implementation
 * @since 3.2.1
 */
@XStreamAlias("advanced-amazon-sqs-implementation")
public class AdvancedSQSImplementation extends AmazonSQSImplementation {

  @NotNull
  @AutoPopulated
  @Valid
  private KeyValuePairSet clientConfigurationProperties;

  @AdvancedConfig
  @Valid
  private RetryPolicyFactory retryPolicy;
  @Valid
  @AdvancedConfig
  private CustomEndpoint customEndpoint;
    
  public AdvancedSQSImplementation() {
    setClientConfigurationProperties(new KeyValuePairSet());
  }

  @Override
  protected SQSConnectionFactory build() throws Exception {
    
    ClientConfiguration cc = ClientConfigurationBuilder.build(getClientConfigurationProperties())
        .withRetryPolicy(retryPolicy());
    AmazonSQS sqsClient = getSqsClientFactory().createClient(authentication().getAWSCredentials(), cc, endpointBuilder());
    return new SQSConnectionFactory(newProviderConfiguration(), sqsClient);
  }
  
  @Override
  protected EndpointBuilder endpointBuilder(){
    return getCustomEndpoint() != null && getCustomEndpoint().isConfigured() ? getCustomEndpoint()
        : new RegionOnly();
  }
  
  public KeyValuePairSet getClientConfigurationProperties() {
    return clientConfigurationProperties;
  }

  public void setClientConfigurationProperties(KeyValuePairSet kvps) {
    this.clientConfigurationProperties = Args.notNull(kvps, "clientConfigurationProperties");
  }

  public RetryPolicyFactory getRetryPolicy() {
    return retryPolicy;
  }

  public void setRetryPolicy(RetryPolicyFactory retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  public CustomEndpoint getCustomEndpoint() {
    return customEndpoint;
  }

  /**
   * Set a custom endpoint for this connection.
   * <p>
   * Generally speaking, you don't need to configure this; use {@link #setRegion(String)} instead. This is only required if you are
   * planning to use a non-standard service endpoint such as <a href="https://github.com/localstack/localstack">localstack</a> to
   * provide AWS services. Explicitly configuring this means that your {@link #setRegion(String)} will have no effect (i.e.
   * {@code AwsClientBuilder#setRegion(String)} will never be invoked.
   * </p>
   * 
   * @param endpoint
   */
  public void setCustomEndpoint(CustomEndpoint endpoint) {
    this.customEndpoint = endpoint;
  }
  
  public AdvancedSQSImplementation withCustomEndpoint(CustomEndpoint endpoint) {
    setCustomEndpoint(endpoint);
    return this;
  }
  
  RetryPolicy retryPolicy() throws Exception {
    return ObjectUtils.defaultIfNull(getRetryPolicy(), new DefaultRetryPolicyFactory()).build();
  }

}
