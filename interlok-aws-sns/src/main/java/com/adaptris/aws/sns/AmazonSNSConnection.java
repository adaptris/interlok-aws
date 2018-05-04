package com.adaptris.aws.sns;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
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
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy"})
public class AmazonSNSConnection extends AWSConnection {

  private transient AmazonSNSClient snsClient;

  public AmazonSNSConnection() {
  }

  public AmazonSNSConnection(AWSAuthentication auth, KeyValuePairSet cfg) {
    this();
    setAuthentication(auth);
    setClientConfiguration(cfg);
  }

  @Override
  protected void prepareConnection() throws CoreException {
  }

  @Override
  protected void initConnection() throws CoreException {
    try {
      AWSCredentials creds = authentication().getAWSCredentials();
      ClientConfiguration cc = ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      AmazonSNSClientBuilder builder = AmazonSNSClientBuilder.standard().withClientConfiguration(cc);
      if (creds != null) {
        builder.withCredentials(new AWSStaticCredentialsProvider(creds));
      }
      snsClient = (AmazonSNSClient) builder.withRegion(getRegion()).build();
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  protected void startConnection() throws CoreException {
  }

  @Override
  protected void stopConnection() {
  }

  @Override
  protected void closeConnection() {
    if (snsClient != null) {
      snsClient.shutdown();
      snsClient = null;
    }
  }

  public AmazonSNSClient amazonClient() {
    return snsClient;
  }
}
