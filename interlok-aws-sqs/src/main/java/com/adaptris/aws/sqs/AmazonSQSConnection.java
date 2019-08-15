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

package com.adaptris.aws.sqs;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws.AWSConnection;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon SQS.
 * 
 * <p>
 * This class directly exposes almost all the getter and setters that are available in {@link ClientConfiguration} via the
 * {@link #getClientConfiguration()} property for maximum flexibility in configuration.
 * </p>
 * <p>
 * The key from the <code>client-configuration</code> element should match the name of the underlying ClientConfiguration
 * property; so if you wanted to control the user-agent you would do :
 * </p>
 * <pre>
 * {@code 
 *   <client-configuration>
 *     <key-value-pair>
 *        <key>UserAgent</key>
 *        <value>My User Agent</value>
 *     </key-value-pair>
 *   </client-configuration>
 * }
 * </pre>
 * 
 * 
 * @config amazon-sqs-connection
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting native connectivity to Amazon SQS", tag = "connections,amazon,sqs",
    recommended = {AmazonSQSConsumer.class, AmazonSQSProducer.class})
@DisplayOrder(order =
{
    "region", "authentication", "clientConfiguration", "retryPolicy", "sqsClientFactory", "customEndpoint"
})
public class AmazonSQSConnection extends AWSConnection {

  /**
   * How to create the SQS client and set parameters.
   */
  @NotNull
  @AutoPopulated
  @Valid
  @InputFieldDefault(value = "UnbufferedSQSClientFactory")
  @Getter
  @Setter
  @NonNull
  private SQSClientFactory sqsClientFactory;

  private transient AmazonSQSAsync sqsClient;


  public AmazonSQSConnection() {
    setSqsClientFactory(new UnbufferedSQSClientFactory());
    setClientConfiguration(new KeyValuePairSet());
  }

  @Override
  protected void prepareConnection() throws CoreException {
    // nothing to do.
  }


  @Override
  protected void closeConnection() {
    sqsClient = null;
  }

  @Override
  protected synchronized void initConnection() throws CoreException {
    try {
      ClientConfiguration cc = ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      sqsClient = getSqsClientFactory().createClient(credentialsProvider().build(), cc, endpointBuilder());
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  protected void startConnection() throws CoreException {
    // Nothing to do here, SQSClient isn't really a connection
  }

  @Override
  protected void stopConnection() {
    if(sqsClient != null) {
      sqsClient.shutdown();
      sqsClient = null;
    }
  }

  /**
   * Access method for getting the synchronous SQSClient for producer/consumer
   */
  AmazonSQS getSyncClient() throws CoreException {
    if(sqsClient == null) {
      throw new CoreException("Amazon SQS Connection is not initialized");
    }
    
    return sqsClient;
  }

  /**
   * Access method for getting the asynchronous SQSClient for producer/consumer
   */
  AmazonSQSAsync getASyncClient() throws CoreException {
    if(sqsClient == null) {
      throw new CoreException("Amazon SQS Connection is not initialized");
    }
    
    return sqsClient;
  }
}
