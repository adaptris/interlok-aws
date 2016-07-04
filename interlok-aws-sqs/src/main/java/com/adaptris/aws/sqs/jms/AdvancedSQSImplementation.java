package com.adaptris.aws.sqs.jms;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.util.KeyValuePairSet;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.SQSConnectionFactory.Builder;
import com.amazonaws.ClientConfiguration;
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
  private RetryPolicyBuilder retryPolicyBuilder;


  public AdvancedSQSImplementation() {
    setClientConfigurationProperties(new KeyValuePairSet());
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
        ClientConfigurationBuilder.configure(builder.getClientConfiguration(), getClientConfigurationProperties());
    if (getRetryPolicyBuilder() != null) {
      cc = cc.withRetryPolicy(getRetryPolicyBuilder().build());
    }
    builder.setClientConfiguration(cc);
    return builder;
  }

  public KeyValuePairSet getClientConfigurationProperties() {
    return clientConfigurationProperties;
  }

  public void setClientConfigurationProperties(KeyValuePairSet kvps) {
    this.clientConfigurationProperties = Args.notNull(kvps, "clientConfigurationProperties");
  }

  public RetryPolicyBuilder getRetryPolicyBuilder() {
    return retryPolicyBuilder;
  }

  /**
   * Set the builder for the {@link com.amazonaws.retry.RetryPolicy}.
   * 
   * @param b the builder.
   */
  public void setRetryPolicyBuilder(RetryPolicyBuilder b) {
    this.retryPolicyBuilder = b;
  }


}
