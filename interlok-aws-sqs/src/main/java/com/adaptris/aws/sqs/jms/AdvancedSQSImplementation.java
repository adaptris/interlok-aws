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

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.util.Args;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.Removal;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.RetryPolicyFactory;
import com.adaptris.core.CoreException;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.util.KeyValuePairSet;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.SQSConnectionFactory.Builder;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.RetryPolicy;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * JMS VendorImplementation for Amazon SQS.
 * <p>
 * This VendorImplementation uses the Amazon SQS JMS compatibility layer. When using this class, do not use the AmazonSQS Producer
 * and Consumer classes. Use regular JMS consumers and producers instead.
 * </p>
 * <p>
 * This vendor implementation class directly exposes almost all the getter and setters that are available in the
 * {@link Builder#getClientConfiguration()}
 * for maximum flexibility in configuration.
 * </p>
 * <p>
 * The key from the <code>connection-factory-properties</code> element should match the name of the underlying ClientConfiguration
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
 * @license STANDARD
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
  @Deprecated
  private RetryPolicyBuilder retryPolicyBuilder;

  private RetryPolicyFactory retryPolicy;

  public AdvancedSQSImplementation() {
    setClientConfigurationProperties(new KeyValuePairSet());
  }

  @Override
  public void prepare() throws CoreException {
    super.prepare();
    if (getRetryPolicyBuilder() != null && getRetryPolicy() == null) {
      log.warn("retry-policy-builder is deprecated; use retry-policy instead");
    }
  }

  @Override
  public ConnectionFactory createConnectionFactory() throws JMSException {
    SQSConnectionFactory connectionFactory = null;
    try {
      connectionFactory = configure(builder()).build();
    } catch (PasswordException e) {
      rethrowJMSException(e);
    } catch (Exception e) {
      rethrowJMSException("Exception configuring client configuration", e);
    }

    return connectionFactory;
  }

  Builder configure(Builder builder) throws Exception {
    ClientConfiguration cc =
        ClientConfigurationBuilder.configure(builder.getClientConfiguration(), getClientConfigurationProperties())
            .withRetryPolicy(retryPolicy());
    builder.setClientConfiguration(cc);
    return builder;
  }

  public KeyValuePairSet getClientConfigurationProperties() {
    return clientConfigurationProperties;
  }

  public void setClientConfigurationProperties(KeyValuePairSet kvps) {
    this.clientConfigurationProperties = Args.notNull(kvps, "clientConfigurationProperties");
  }

  /**
   * 
   * @deprecated since 3.6.4 use a {@link RetryPolicyFactory} instead.
   */
  @Deprecated
  @Removal(version = "3.9.0",
      message = "use PluggableRetryPolicyFactory as part of the connection instead")
  public RetryPolicyBuilder getRetryPolicyBuilder() {
    return retryPolicyBuilder;
  }

  /**
   * Set the builder for the {@link com.amazonaws.retry.RetryPolicy}.
   * 
   * @deprecated since 3.6.4 use a {@link RetryPolicyFactory} instead.
   * @param b the builder.
   */
  @Deprecated
  @Removal(version = "3.9.0",
      message = "use PluggableRetryPolicyFactory as part of the connection instead")
  public void setRetryPolicyBuilder(RetryPolicyBuilder b) {
    this.retryPolicyBuilder = b;
  }

  public RetryPolicyFactory getRetryPolicy() {
    return retryPolicy;
  }

  public void setRetryPolicy(RetryPolicyFactory retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  @SuppressWarnings("deprecation")
  RetryPolicy retryPolicy() throws Exception {
    if (getRetryPolicyBuilder() != null) {
      return getRetryPolicyBuilder().build();
    }
    return ObjectUtils.defaultIfNull(getRetryPolicy(), new DefaultRetryPolicyFactory()).build();
  }

}
