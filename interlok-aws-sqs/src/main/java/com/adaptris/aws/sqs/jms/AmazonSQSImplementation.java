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

import static com.adaptris.core.jms.JmsUtils.wrapJMSException;
import javax.jms.JMSException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.EndpointBuilder;
import com.adaptris.aws.RegionEndpoint;
import com.adaptris.aws.RetryPolicyFactory;
import com.adaptris.aws.sqs.SQSClientFactory;
import com.adaptris.aws.sqs.UnbufferedSQSClientFactory;
import com.adaptris.core.jms.VendorImplementationBase;
import com.adaptris.core.jms.VendorImplementationImp;
import com.adaptris.util.KeyValuePairSet;
import com.adaptris.util.NumberUtils;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
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
 *
 * @config amazon-sqs-implementation
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-implementation")
@DisplayOrder(order = {"region", "prefetchCount", "credentials"})
public class AmazonSQSImplementation extends VendorImplementationImp
    implements AWSCredentialsProviderBuilder.BuilderConfig {

  private static int DEFAULT_PREFETCH_COUNT = 10;

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
   * The maximum number of messages to retrieve from the Amazon SQS queue per request.
   *
   */
  @AdvancedConfig
  @Getter
  @Setter
  @InputFieldDefault(value = "10")
  private Integer prefetchCount;

  /**
   * How to create the SQS client and set parameters.
   *
   */
  @NotNull
  @AutoPopulated
  @Valid
  @InputFieldDefault(value = "UnbufferedSQSClientFactory")
  @Getter
  @Setter
  @NonNull
  private SQSClientFactory sqsClientFactory;

  private transient SQSConnectionFactory connectionFactory = null;

  public AmazonSQSImplementation() {
    setSqsClientFactory(new UnbufferedSQSClientFactory());
  }

  @Override
  public SQSConnectionFactory createConnectionFactory() throws JMSException {
    try {
      if (connectionFactory == null) connectionFactory = build();
    }
    catch (Exception e) {
      throw wrapJMSException(e);
    }
    return connectionFactory;
  }

  protected SQSConnectionFactory build() throws Exception {
    ClientConfiguration cc = buildClientConfiguration();
    AmazonSQS sqsClient =
        getSqsClientFactory().createClient(credentialsProvider().build(this), cc,
            endpointBuilder());
    return new SQSConnectionFactory(newProviderConfiguration(), sqsClient);
  }

  protected ClientConfiguration buildClientConfiguration() throws Exception {
    return ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
  }

  @SuppressWarnings("deprecation")
  protected AWSCredentialsProviderBuilder credentialsProvider() {
    return AWSCredentialsProviderBuilder.defaultIfNull(getCredentials());
  }

  @Override
  public boolean connectionEquals(VendorImplementationBase arg0) {
    return this == arg0;
  }

  protected int prefetchCount() {
    return NumberUtils.toIntDefaultIfNull(getPrefetchCount(), DEFAULT_PREFETCH_COUNT);
  }


  public <T extends AmazonSQSImplementation> T withClientFactory(SQSClientFactory fac) {
    setSqsClientFactory(fac);
    return (T) this;
  }


  public <T extends AmazonSQSImplementation> T withCredentialsProviderBuilder(AWSCredentialsProviderBuilder builder) {
    setCredentials(builder);
    return (T) this;
  }


  @Override
  public EndpointBuilder endpointBuilder() {
    return new RegionEndpoint(getRegion());
  }

  @Override
  public RetryPolicyFactory retryPolicy() {
    return new DefaultRetryPolicyFactory();
  }

  @Override
  public KeyValuePairSet clientConfiguration() {
    return new KeyValuePairSet();
  }


  protected ProviderConfiguration newProviderConfiguration() {
    return new ProviderConfiguration().withNumberOfMessagesToPrefetch(prefetchCount());
  }
}
