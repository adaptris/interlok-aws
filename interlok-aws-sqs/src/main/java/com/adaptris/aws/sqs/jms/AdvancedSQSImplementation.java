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
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.EndpointBuilder;
import com.adaptris.aws.RegionEndpoint;
import com.adaptris.aws.RetryPolicyFactory;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

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
@DisplayOrder(order = {"region", "prefetchCount", "credentials"})
public class AdvancedSQSImplementation extends AmazonSQSImplementation {

  /**
   * Any other properties you wish to set on the client.
   *
   * @see ClientConfigurationBuilder
   */
  @NotNull
  @AutoPopulated
  @Valid
  @NonNull
  @Getter
  @Setter
  private KeyValuePairSet clientConfigurationProperties;

  /**
   * The retry policy if required.
   *
   */
  @AdvancedConfig
  @Valid
  @Getter
  @Setter
  private RetryPolicyFactory retryPolicy;
  /**
   * Set a custom endpoint for this connection.
   * <p>
   * Generally speaking, you don't need to configure this; use {@link #setRegion(String)} instead.
   * This is only required if you are planning to use a non-standard service endpoint such as
   * <a href="https://github.com/localstack/localstack">localstack</a> to provide AWS services.
   * Explicitly configuring this means that your {@link #setRegion(String)} will have no effect (i.e.
   * {@code AwsClientBuilder#setRegion(String)} will never be invoked.
   * </p>
   *
   */
  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private CustomEndpoint customEndpoint;

  public AdvancedSQSImplementation() {
    setClientConfigurationProperties(new KeyValuePairSet());
  }

  @Override
  public KeyValuePairSet clientConfiguration() {
    return ObjectUtils.defaultIfNull(getClientConfigurationProperties(), new KeyValuePairSet());
  }

  @Override
  public EndpointBuilder endpointBuilder() {
    return getCustomEndpoint() != null && getCustomEndpoint().isConfigured() ? getCustomEndpoint()
        : new RegionEndpoint(getRegion());
  }

  public AdvancedSQSImplementation withCustomEndpoint(CustomEndpoint endpoint) {
    setCustomEndpoint(endpoint);
    return this;
  }

  @Override
  public RetryPolicyFactory retryPolicy() {
    return ObjectUtils.defaultIfNull(getRetryPolicy(), new DefaultRetryPolicyFactory());
  }

}
