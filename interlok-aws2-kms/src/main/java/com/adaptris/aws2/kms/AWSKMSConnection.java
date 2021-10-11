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

package com.adaptris.aws2.kms;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws2.AWSConnection;
import com.adaptris.aws2.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon KMS
 *
 * <p>
 * This class directly exposes almost all the getter and setters that are available in {@link ClientOverrideConfiguration} via the
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
 * @config aws2-kms-connection
 */
@XStreamAlias("aws2-kms-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to Amazon KMS", tag = "connections,amazon,aws2,kms",
    since = "3.10.1")
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy",
    "customEndpoint"})
@NoArgsConstructor
public class AWSKMSConnection extends AWSConnection implements ClientWrapper<KmsClient> {

  private transient KmsClient kms;

  @Override
  protected void prepareConnection() throws CoreException {
  }

  @Override
  protected void initConnection() throws CoreException {
    KmsClientBuilder builder = createBuilder();
    kms = builder.build();
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

  protected KmsClientBuilder createBuilder() throws CoreException {
    KmsClientBuilder builder;
    try {
      ClientOverrideConfiguration cc = ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());

      builder = KmsClient.builder();
      builder.overrideConfiguration(cc);
      if (getRegion() != null) {
        builder.region(Region.of(getRegion()));
      }
      builder.credentialsProvider(credentialsProvider().build(this));
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    return builder;
  }

  @Override
  public KmsClient awsClient() throws Exception {
    return kms;
  }

}
