package com.adaptris.aws.sqs;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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
 * @license STANDARD
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection for supporting native connectivity to Amazon SQS", tag = "connections,amazon,sqs",
    recommended = {AmazonSQSConsumer.class, AmazonSQSProducer.class})
public class AmazonSQSConnection extends AdaptrisConnectionImp {

  @NotNull
  private String region;
  
  @Deprecated
  private String accessKey;
  
  @InputFieldHint(style="PASSWORD")
  @Deprecated
  private String secretKey;

  @Valid
  @NotNull
  @AutoPopulated
  private AWSAuthentication authentication;
  
  @NotNull
  @AutoPopulated
  @Valid
  private SQSClientFactory sqsClientFactory;

  @NotNull
  @AutoPopulated
  @Valid
  private KeyValuePairSet clientConfiguration;

  private transient AmazonSQSAsync sqsClient;


  public AmazonSQSConnection() {
    setAuthentication(new DefaultAWSAuthentication());
    setSqsClientFactory(new UnbufferedSQSClientFactory());
    setClientConfiguration(new KeyValuePairSet());
  }


  @Override
  protected void prepareConnection() throws CoreException {
    if(getAuthentication() == null && StringUtils.isNotEmpty(getAccessKey())) {
      throw new CoreException("'authentication' and 'accessKey' are both configured. 'accessKey' and 'secretKey' are deprecated, please use 'authentication' only");
    }
    
    if(StringUtils.isNotEmpty(getAccessKey()) && StringUtils.isNotEmpty(getSecretKey())) {
      AWSKeysAuthentication auth = new AWSKeysAuthentication();
      auth.setAccessKey(getAccessKey());
      auth.setSecretKey(getSecretKey());
      setAuthentication(auth);
    }
  }


  @Override
  protected void closeConnection() {
    sqsClient = null;
  }

  @Override
  protected synchronized void initConnection() throws CoreException {
    try {
      ClientConfiguration cc = ClientConfigurationBuilder.build(getClientConfiguration());
      sqsClient = getSqsClientFactory().createClient(authentication.getAWSCredentials(), cc, region);
    } catch (Exception e) {
      throw new CoreException(e);
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
  
  /**
   * The AWS region endpoint for the account
   * 
   * @return region
   */
  public String getRegion() {
    return region;
  }

  /**
   * The AWS region endpoint for the account
   * 
   * @param region
   */
  public void setRegion(String region) {
    this.region = region;
  }

  /**
   * The access key for the AWS account
   * 
   * @return accessKey
   */
  @Deprecated
  public String getAccessKey() {
    return accessKey;
  }

  /**
   * The access key for the AWS account
   * 
   * @param accessKey
   */
  @Deprecated
  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  /**
   * The secret key for the AWS account. Can be encoded.
   * 
   * @return secretKey
   */
  @Deprecated
  public String getSecretKey() {
    return secretKey;
  }

  /**
   * The secret key for the AWS account. Can be encoded.
   * 
   * @param secretKey
   */
  @Deprecated
  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  /**
   * How to create the SQS client and set parameters.
   */
  public void setSqsClientFactory(SQSClientFactory sqsClientFactory) {
    this.sqsClientFactory = sqsClientFactory;
  }
  
  public SQSClientFactory getSqsClientFactory() {
    return sqsClientFactory;
  }


  public AWSAuthentication getAuthentication() {
    return authentication;
  }

  /**
   * The authentication method to use
   */
  public void setAuthentication(AWSAuthentication authentication) {
    this.authentication = authentication;
  }


  /**
   * @return the configurationBuilder
   */
  public KeyValuePairSet getClientConfiguration() {
    return clientConfiguration;
  }


  /**
   * @param b the configurationBuilder to set
   */
  public void setClientConfiguration(KeyValuePairSet b) {
    this.clientConfiguration = Args.notNull(b, "configurationBuilder");
  }
}
