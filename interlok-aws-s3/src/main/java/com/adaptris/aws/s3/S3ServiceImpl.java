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

import javax.validation.Valid;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.Removal;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.ConnectedService;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Abstract implemention of {@link S3Service}
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
 * @author lchan
 *
 */
public abstract class S3ServiceImpl extends ServiceImp implements ConnectedService {

  @Valid
  @Deprecated
  @AdvancedConfig
  private AWSAuthentication authentication;

  @Valid
  @Deprecated
  @AdvancedConfig
  private KeyValuePairSet clientConfiguration;

  @Valid
  private AdaptrisConnection connection;
  private transient boolean warningLogged;
  
  public S3ServiceImpl() {
  }

  @Override
  public void prepare() throws CoreException {
    handleDeprecatedConfig();
    LifecycleHelper.prepare(getConnection());
  }

  protected void handleDeprecatedConfig() {
    if (getAuthentication() != null) {
      log.warn("authentication is deprecated); use amazon-s3-connection instead");
    }
    if (getClientConfiguration() != null) {
      log.warn("client-configuration is deprecated); use amazon-s3-connection instead");
    }
    if (getConnection() == null) {
      LoggingHelper.logWarning(warningLogged, ()-> {warningLogged = true;}, 
          "No Connection; using legacy authentication and client-configuration");      
      setConnection(new AmazonS3Connection(getAuthentication(), getClientConfiguration()));
    }
  }
  
  @Override
  protected void initService() throws CoreException {
    LifecycleHelper.init(getConnection());
  }

  @Override
  public void start() throws CoreException {
    super.start();
    LifecycleHelper.start(getConnection());
  }

  @Override
  public void stop() {
    super.stop();
    LifecycleHelper.stop(getConnection());
  }

  @Override
  protected void closeService() {
    LifecycleHelper.close(getConnection());
  }

  @Deprecated
  @Removal(version="3.9.0")
  protected AmazonS3Client amazonClient() {
    return getConnection().retrieveConnection(AmazonS3Connection.class).amazonClient();
  }

  /**
   * @deprecated since 3.6.4 use a {@link AmazonS3Connection} instead.
   */
  @Deprecated
  @Removal(version = "3.9.0", message = "use AmazonS3Connection as the connection instead")
  public AWSAuthentication getAuthentication() {
    return authentication;
  }

  /**
   * The authentication method to use
   * 
   * @param a the authentication to use.
   * @deprecated since 3.6.4 use a {@link AmazonS3Connection} instead.
   */
  @Deprecated
  @Removal(version = "3.9.0", message = "use AmazonS3Connection as the connection instead")
  public void setAuthentication(AWSAuthentication a) {
    this.authentication = a;
  }


  /**
   * @return the configurationBuilder
   * @deprecated since 3.6.4 use a {@link AmazonS3Connection} instead.
   */
  @Deprecated
  @Removal(version = "3.9.0", message = "use AmazonS3Connection as the connection instead")
  public KeyValuePairSet getClientConfiguration() {
    return clientConfiguration;
  }


  /**
   * @param b the configurationBuilder to set
   * @deprecated since 3.6.4 use a {@link AmazonS3Connection} instead.
   */
  @Deprecated
  @Removal(version = "3.9.0", message = "use AmazonS3Connection as the connection instead")
  public void setClientConfiguration(KeyValuePairSet b) {
    this.clientConfiguration = b;
  }

  public AdaptrisConnection getConnection() {
    return connection;
  }

  /**
   * Set the connection to use to connect to S3.
   * 
   * @param connection the connection.
   */
  public void setConnection(AdaptrisConnection connection) {
    this.connection = connection;
  }
}
