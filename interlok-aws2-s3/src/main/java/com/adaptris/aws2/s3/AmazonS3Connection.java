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

package com.adaptris.aws2.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws2.AWSConnection;
import com.adaptris.aws2.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon S3.
 *
 * <p>
 * This class directly exposes almost all the getter and setters that are available in {@link software.amazon.awssdk.core.client.config.ClientOverrideConfiguration} via the
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
 * @config amazon-s3-connection
 * @since 4.3.0
 */
@XStreamAlias("aws2-amazon-s3-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to Amazon S3", tag = "connections,amazon,s3",
    recommended = {S3Service.class}, since = "4.3.0")
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy",
    "customEndpoint", "forcePathStyleAccess"})
public class AmazonS3Connection extends AWSConnection implements ClientWrapper {

  private transient S3Client s3;

  /**
   * Configures the client to use path-style access for all requests.
   * <p>
   * The default behaviour is to detect which access style to use based on the configured endpoint (an
   * IP will result in path-style access) and the bucket being accessed (some buckets are not valid
   * DNS names). Setting this flag will result in path-style access being used for all requests.
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "not configured")
  @Getter
  @Setter
  private Boolean forcePathStyleAccess;

  public AmazonS3Connection() {
  }

  @Override
  protected void prepareConnection() throws CoreException {
  }

  @Override
  protected void initConnection() throws CoreException {
    S3ClientBuilder builder = createBuilder();
    s3 = builder.build();
  }

  protected S3ClientBuilder createBuilder() throws CoreException {
    S3ClientBuilder builder;
    try {
      builder = S3Client.builder();

      S3Configuration.Builder s3ConfigurationBuilder = S3Configuration.builder();

      if (getForcePathStyleAccess() != null) {
        s3ConfigurationBuilder.pathStyleAccessEnabled(getForcePathStyleAccess());
      }

      if (getRegion() != null) {
        builder.region(Region.of(getRegion()));
      }

      builder.serviceConfiguration(s3ConfigurationBuilder.build());
      builder.overrideConfiguration(ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy()));

      builder.credentialsProvider(credentialsProvider().build(this));
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    return builder;
  }


  @Override
  protected void startConnection() throws CoreException {
    // Nothing to do
  }

  @Override
  protected void stopConnection() {
    // Nothing to do
  }

  @Override
  protected void closeConnection() {
    shutdownQuietly(s3);
    s3 = null;
  }

  protected static void shutdownQuietly(S3Client client) {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public S3Client amazonClient() {
    return s3;
  }

}
