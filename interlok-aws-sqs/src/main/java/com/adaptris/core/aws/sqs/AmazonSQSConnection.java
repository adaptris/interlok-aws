package com.adaptris.core.aws.sqs;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * {@linkplain AdaptrisConnection} implementation for Amazon SQS.
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
  
  @NotNull
  @Deprecated
  private String accessKey;
  
  @NotNull
  @InputFieldHint(style="PASSWORD")
  @Deprecated
  private String secretKey;
  
  private AWSAuthentication authentication;
  
  @NotNull
  @AutoPopulated
  private SQSClientFactory sqsClientFactory;

  private transient AmazonSQSAsync sqsClient;

  public AmazonSQSConnection() {
    setSqsClientFactory(new UnbufferedSQSClientFactory());
  }


  @Override
  protected void prepareConnection() throws CoreException {
    if(getAuthentication() == null && StringUtils.isNotEmpty(getAccessKey()) && StringUtils.isNotEmpty(getSecretKey())) {
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
      sqsClient = getSqsClientFactory().createClient(authentication.getAWSCredentials());
    } catch (Exception e) {
      throw new CoreException(e);
    }
    
    Region rgn = AwsHelper.formatRegion(region);
    sqsClient.setRegion(rgn);
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
}
