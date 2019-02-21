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

package com.adaptris.aws.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.AWSConnection;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon S3.
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
 * @config amazon-s3-connection
 */
@XStreamAlias("amazon-s3-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting connectivity to Amazon S3", tag = "connections,amazon,s3",
    recommended = {S3Service.class})
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy",
    "customEndpoint", "forcePathStyleAccess"})
public class AmazonS3Connection extends AWSConnection implements ClientWrapper {

  private transient AmazonS3Client s3;
  private transient TransferManager transferManager;

  @AdvancedConfig
  @InputFieldDefault(value = "not configured")
  private Boolean forcePathStyleAccess;

  public AmazonS3Connection() {
  }

  public AmazonS3Connection(AWSAuthentication auth, KeyValuePairSet cfg) {
    this();
    setAuthentication(auth);
    setClientConfiguration(cfg);
  }

  @Override
  protected void prepareConnection() throws CoreException {
  }

  @Override
  protected void initConnection() throws CoreException {
    AmazonS3ClientBuilder builder = createBuilder();
    s3 = (AmazonS3Client) builder.build();
    transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
  }

  protected AmazonS3ClientBuilder createBuilder() throws CoreException {
    AmazonS3ClientBuilder builder = null;
    try {
      AWSCredentials creds = authentication().getAWSCredentials();
      ClientConfiguration cc =
          ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      builder = endpointBuilder().rebuild(AmazonS3ClientBuilder.standard().withClientConfiguration(cc));
      if (getForcePathStyleAccess() != null) {
        builder.setPathStyleAccessEnabled(getForcePathStyleAccess());
      }
      if (creds != null) {
        builder.withCredentials(new AWSStaticCredentialsProvider(creds));
      }
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
    shutdownQuietly(transferManager);
    shutdownQuietly(s3);
    transferManager = null;
    s3 = null;
  }

  protected static void shutdownQuietly(TransferManager tm) {
    if (tm != null) {
      tm.shutdownNow(false);
    }
  }

  protected static void shutdownQuietly(AmazonS3Client client) {
    if (client != null) {
      client.shutdown();
    }
  }

  @Override
  public AmazonS3Client amazonClient() {
    return s3;
  }

  @Override
  public TransferManager transferManager() {
    return transferManager;
  }

  public Boolean getForcePathStyleAccess() {
    return forcePathStyleAccess;
  }

  /**
   * Configures the client to use path-style access for all requests.
   * <p>
   * The default behaviour is to detect which access style to use based on the configured endpoint
   * (an IP will result in path-style access) and the bucket being accessed (some buckets are not
   * valid DNS names). Setting this flag will result in path-style access being used for all
   * requests.
   * </p>
   * 
   * @param b default is not configured
   */
  public void setForcePathStyleAccess(Boolean b) {
    this.forcePathStyleAccess = b;
  }



}
