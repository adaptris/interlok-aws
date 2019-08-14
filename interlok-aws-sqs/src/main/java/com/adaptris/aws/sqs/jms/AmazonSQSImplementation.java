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
import org.apache.commons.lang3.StringUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.Removal;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.EndpointBuilder;
import com.adaptris.aws.sqs.SQSClientFactory;
import com.adaptris.aws.sqs.UnbufferedSQSClientFactory;
import com.adaptris.core.jms.VendorImplementationBase;
import com.adaptris.core.jms.VendorImplementationImp;
import com.adaptris.util.KeyValuePairSet;
import com.adaptris.util.NumberUtils;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
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
public class AmazonSQSImplementation extends VendorImplementationImp {

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
    ClientConfiguration cc = clientConfiguration();
    AmazonSQS sqsClient = getSqsClientFactory().createClient(credentialsProvider().build(), cc, endpointBuilder());
    return new SQSConnectionFactory(newProviderConfiguration(), sqsClient);
  }

  protected ClientConfiguration clientConfiguration() throws Exception {
    return ClientConfigurationBuilder.build(new KeyValuePairSet());
  }

  @SuppressWarnings("deprecation")
  protected AWSCredentialsProviderBuilder credentialsProvider() {
    return AWSCredentialsProviderBuilder.providerWithWarning(getClass().getCanonicalName(), getAuthentication(), getCredentials());
  }


  @Override
  public boolean connectionEquals(VendorImplementationBase arg0) {
    return this == arg0;
  }

  protected int prefetchCount() {
    return NumberUtils.toIntDefaultIfNull(getPrefetchCount(), DEFAULT_PREFETCH_COUNT);
  }


  @Deprecated
  @Removal(version = "3.11.0")
  public <T extends AmazonSQSImplementation> T withAuthentication(AWSAuthentication a) {
    setAuthentication(a);
    return (T) this;
  }
  
  public <T extends AmazonSQSImplementation> T withClientFactory(SQSClientFactory fac) {
    setSqsClientFactory(fac);
    return (T) this;
  }
  

  public <T extends AmazonSQSImplementation> T withCredentialsProviderBuilder(AWSCredentialsProviderBuilder builder) {
    setCredentials(builder);
    return (T) this;
  }


  protected EndpointBuilder endpointBuilder() {
    return new RegionOnly();
  }
  
  protected ProviderConfiguration newProviderConfiguration() {
    return new ProviderConfiguration().withNumberOfMessagesToPrefetch(prefetchCount());
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
