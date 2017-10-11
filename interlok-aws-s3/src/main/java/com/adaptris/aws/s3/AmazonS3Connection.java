package com.adaptris.aws.s3;

import javax.validation.Valid;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.RetryPolicyFactory;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
public class AmazonS3Connection extends AdaptrisConnectionImp {

  private transient AmazonS3Client s3;
  private String region;
  
  @Valid
  private AWSAuthentication authentication;

  @Valid
  private KeyValuePairSet clientConfiguration;

  @Valid
  @AdvancedConfig
  private RetryPolicyFactory retryPolicy;

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
    s3 = null;
  }

  public AmazonS3Client amazonClient() {
    return s3;
  }

  public AWSAuthentication getAuthentication() {
    return authentication;
  }

  AWSAuthentication authentication() {
    return getAuthentication() != null ? getAuthentication() : new DefaultAWSAuthentication();
  }

  /**
   * The authentication method to use
   * 
   * @param auth the authentication to use, defaults to {@code DefaultAWSAuthentication} if not specified.
   */
  public void setAuthentication(AWSAuthentication auth) {
    this.authentication = auth;
  }

  /**
   * @return the clientConfiguration
   */
  public KeyValuePairSet getClientConfiguration() {
    return clientConfiguration;
  }

  /**
   * @param b the clientConfiguration to set
   */
  public void setClientConfiguration(KeyValuePairSet b) {
    this.clientConfiguration = b;
  }

  KeyValuePairSet clientConfiguration() {
    return getClientConfiguration() != null ? getClientConfiguration() : new KeyValuePairSet();
  }

  public RetryPolicyFactory getRetryPolicy() {
    return retryPolicy;
  }

  /**
   * Set the retry policy if required.
   * 
   * @param rp the retry policy, defaults to {@code DefaultRetryPolicyBuilder} if not specified.
   */
  public void setRetryPolicy(RetryPolicyFactory rp) {
    this.retryPolicy = rp;
  }

  RetryPolicyFactory retryPolicy() {
    return getRetryPolicy() != null ? getRetryPolicy() : new DefaultRetryPolicyFactory();
  }

  public String getRegion() {
    return region;
  }

  /**
   * Set the region for the client.
   * 
   * <p>
   * If the region is not specified, then {@link DefaultAwsRegionProviderChain} is used to determine the region. You can always
   * specify a region using the standard system property {@code aws.region} or via environment variables.
   * </p>
   * 
   * @param s the region.
   */
  public void setRegion(String s) {
    this.region = s;
  }
}
