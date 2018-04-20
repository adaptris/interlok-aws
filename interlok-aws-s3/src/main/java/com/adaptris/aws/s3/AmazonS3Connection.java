package com.adaptris.aws.s3;

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
@DisplayOrder(order = {"region", "authentication", "clientConfiguration", "retryPolicy"})
public class AmazonS3Connection extends AWSConnection implements ClientWrapper {

  private transient AmazonS3Client s3;
  private transient TransferManager transferManager;

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
    try {
      AWSCredentials creds = authentication().getAWSCredentials();
      ClientConfiguration cc = ClientConfigurationBuilder.build(clientConfiguration(), retryPolicy());
      AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withClientConfiguration(cc);
      if (creds != null) {
        builder.withCredentials(new AWSStaticCredentialsProvider(creds));
      }
      s3 = (AmazonS3Client) builder.withRegion(getRegion()).build();
      transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
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
    if (transferManager != null) {
      transferManager.shutdownNow(false);
      transferManager = null;
    }
    if (s3 != null) {
      s3.shutdown();
      s3 = null;
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


}
