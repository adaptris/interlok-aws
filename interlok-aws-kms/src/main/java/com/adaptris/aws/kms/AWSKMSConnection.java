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

package com.adaptris.aws.kms;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.AWSConnection;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon KMS
 *
 * <p>
 * This class directly exposes almost all the getter and setters that are available in {@link ClientConfiguration} via the
 * {@link #getClientConfiguration()} property for maximum flexibility in configuration.
 * </p>
 * <p>
 * The key from the <code>client-configuration</code> element should match the name of the underlying ClientConfiguration property;
 * so if you wanted to control the user-agent you would do :
 * </p>
 *
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
 * @config aws-kms-connection
 */
@XStreamAlias("aws-kms-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to Amazon KMS", tag = "connections,amazon,aws,kms",
    since = "3.10.1")
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy",
    "customEndpoint"})
@NoArgsConstructor
public class AWSKMSConnection extends AWSConnection implements ClientWrapper<AWSKMSClient> {

  private transient AWSKMSClient kms;

  @Override
  protected void prepareConnection() throws CoreException {
  }

  @Override
  protected void initConnection() throws CoreException {
    AWSKMSClientBuilder builder = createBuilder();
    kms = (AWSKMSClient) builder.build();
  }



  @Override
  protected void startConnection() throws CoreException {  }

  @Override
  protected void stopConnection() {
    // Nothing to do
  }


  @Override
  protected void closeConnection() {
    ClientWrapper.shutdownQuietly(kms);
    kms = null;
  }

  protected AWSKMSClientBuilder createBuilder() throws CoreException {
    AWSKMSClientBuilder builder = null;
    try {
      ClientConfiguration cc =
          ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      builder = endpointBuilder().rebuild(AWSKMSClientBuilder.standard().withClientConfiguration(cc));
      builder.withCredentials(credentialsProvider().build(this));
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    return builder;
  }

  @Override
  public AWSKMSClient awsClient() throws Exception {
    return kms;
  }

}
