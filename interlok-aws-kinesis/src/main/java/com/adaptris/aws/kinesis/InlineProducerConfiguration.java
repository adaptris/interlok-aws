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

package com.adaptris.aws.kinesis;

import java.util.Properties;

import javax.validation.Valid;

import org.apache.commons.lang3.ObjectUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.util.KeyValuePairBag;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon Kinesis using the Kinesis Producer Library.
 * 
 * <p>
 * This derives its connection information from configured properties. The key difference between this and
 * {@link ConnectionFromProperties} is that this allows you to override the {@code AWSCredentialsProvider} with a custom one. Use
 * this if a {@code DefaultAWSCredentialsProviderChain} is not appropriate in your environment.
 * </p>
 * 
 * @config aws-kinesis-kpl-inline-connection
 */
@XStreamAlias("aws-kinesis-kpl-inline-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to AWS Kinesis", tag = "connections,amazon,aws,kinesis")
@DisplayOrder(order = {"config", "credentials", "metricsCredentials"})
public class InlineProducerConfiguration extends ProducerLibraryConnection implements KinesisProducerWrapper {

  @AdvancedConfig
  @AutoPopulated
  private KeyValuePairSet config;
  @Valid
  @InputFieldDefault(value = "static-credentials-builder with a default chain")
  private AWSCredentialsProviderBuilder credentials;
  @AdvancedConfig
  @Valid
  @InputFieldDefault(value = "static-credentials-builder with a default chain")
  private AWSCredentialsProviderBuilder metricsCredentials;

  public InlineProducerConfiguration() {
    setConfig(new KeyValuePairSet());
  }

  @Override
  public synchronized KinesisProducer kinesisProducer() throws Exception {
    if (producer == null) {
      KinesisProducerConfiguration config = KinesisProducerConfiguration.fromProperties(KeyValuePairBag.asProperties(getConfig()));
      config.setCredentialsProvider(credentials());
      config.setMetricsCredentialsProvider(metricsCredentials());
      producer = new KinesisProducer(config);
    }
    return producer;
  }

  public KeyValuePairSet getConfig() {
    return config;
  }

  /**
   * Set any configuration required.
   * <p>
   * Set any configuration that is required. The settings here are directly converted into a {@link Properties} object for use
   * with {@code KinesisProducerConfiguration} via the {@code fromProperties()} method.
   * </p>
   * 
   * @param config the configuration.
   */
  public void setConfig(KeyValuePairSet config) {
    this.config = config;
  }

  public InlineProducerConfiguration withConfig(KeyValuePairSet b) {
    setConfig(b);
    return this;
  }

  public AWSCredentialsProviderBuilder getCredentials() {
    return credentials;
  }

  /**
   * Set the credentials to be used for kinesis
   * 
   * @param c
   */
  public void setCredentials(AWSCredentialsProviderBuilder c) {
    this.credentials = c;
  }

  public InlineProducerConfiguration withCredentials(AWSCredentialsProviderBuilder b) {
    setCredentials(b);
    return this;
  }

  private AWSCredentialsProvider credentials() throws Exception {
    return ObjectUtils.defaultIfNull(getCredentials(), new StaticCredentialsBuilder()).build();
  }

  public AWSCredentialsProviderBuilder getMetricsCredentials() {
    return metricsCredentials;
  }

  /**
   * Set the credentials to be used for metrics.
   * 
   * @param c the credentials for metrics.
   */
  public void setMetricsCredentials(AWSCredentialsProviderBuilder c) {
    this.metricsCredentials = c;
  }

  public InlineProducerConfiguration withMetricsCredentials(AWSCredentialsProviderBuilder b) {
    setMetricsCredentials(b);
    return this;
  }

  public AWSCredentialsProvider metricsCredentials() throws Exception {
    return ObjectUtils.defaultIfNull(getMetricsCredentials(), new StaticCredentialsBuilder()).build();
  }

}
