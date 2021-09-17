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

package com.adaptris.aws.sns;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.AWSConnection;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon SNS.
 *
 * <p>
 * This class directly exposes almost all the getter and setters that are available in {@link ClientConfiguration} via the
 * {@link #getClientConfiguration()} property for maximum flexibility in configuration.
 * </p>
 * <p>
 * The key from the <code>client-configuration</code> element should match the name of the underlying ClientConfiguration property;
 * so if you wanted to control the user-agent you would do :
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
 * @config amazon-sns-connection
 */
@XStreamAlias("amazon-sns-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to Amazon SNS", tag = "connections,amazon,sns",
    recommended = { NotificationProducer.class })
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy", "customEndpoint"})
public class AmazonSNSConnection extends AWSConnection {

  private transient AmazonSNSClient snsClient;

  public AmazonSNSConnection() {
  }

  @Override
  protected void prepareConnection() throws CoreException {
    // Nothing to do
  }

  @Override
  protected void initConnection() throws CoreException {
    try {
      ClientConfiguration cc = ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      AmazonSNSClientBuilder builder = endpointBuilder().rebuild(AmazonSNSClientBuilder.standard().withClientConfiguration(cc));
      builder.withCredentials(credentialsProvider().build(this));
      snsClient = (AmazonSNSClient) builder.build();
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  protected void startConnection() throws CoreException {
    // Nothing to do
  }

  @Override
  protected void stopConnection() {
  }

  @Override
  protected void closeConnection() {
    closeQuietly(snsClient);
    snsClient = null;
  }

  public AmazonSNSClient amazonClient() {
    return snsClient;
  }

  protected static void closeQuietly(AmazonSNSClient c) {
    if (c != null) {
      c.shutdown();
    }
  }
}
