package com.adaptris.aws.sqs.jms;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.aws.AWSAuthentication;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.core.CoreException;
import com.adaptris.core.jms.VendorImplementationBase;
import com.adaptris.core.jms.VendorImplementationImp;
import com.adaptris.core.util.Args;
import com.adaptris.security.exc.AdaptrisSecurityException;
import com.adaptris.security.password.Password;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.SQSConnectionFactory.Builder;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * JMS VendorImplementation for Amazon SQS.
 * <p>
 * This VendorImplementation uses the Amazon SQS JMS compatibility layer. When using this class, do not use the AmazonSQS Producer
 * and Consumer classes. Use regular JMS consumers and producers instead.
 * </p>
 * 
 * @config amazon-sqs-implementation
 * @license STANDARD
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-implementation")
public class AmazonSQSImplementation extends VendorImplementationImp {

  private static int DEFAULT_PREFETCH_COUNT = 10;

  @NotNull
  private String region;

  @Deprecated
  private String accessKey;

  @InputFieldHint(style = "PASSWORD")
  @Deprecated
  private String secretKey;

  @Valid
  private AWSAuthentication authentication;

  @AdvancedConfig
  private Integer prefetchCount;

  
  public AmazonSQSImplementation() {
    setAuthentication(new DefaultAWSAuthentication());
  }

  @Override
  public ConnectionFactory createConnectionFactory() throws JMSException {
    SQSConnectionFactory connectionFactory = null;
    try {
      connectionFactory = builder().build();
    } catch (Exception e) {
      rethrowJMSException(e);
    }

    return connectionFactory;
  }

  protected Builder builder() throws AdaptrisSecurityException {
    Builder builder = SQSConnectionFactory.builder()
        .withRegionName(getRegion())
        .withNumberOfMessagesToPrefetch(prefetchCount());
    
    if(getAuthentication() == null && StringUtils.isNotEmpty(getAccessKey())) {
      throw new AdaptrisSecurityException("'authentication' and 'accessKey' are both configured. 'accessKey' and 'secretKey' are deprecated, please use 'authentication' only");
    }
    
    if(StringUtils.isNotEmpty(getAccessKey()) && StringUtils.isNotEmpty(getSecretKey())) {
      AWSKeysAuthentication auth = new AWSKeysAuthentication();
      auth.setAccessKey(getAccessKey());
      auth.setSecretKey(getSecretKey());
      setAuthentication(auth);
    }
    AWSCredentials creds = getAuthentication().getAWSCredentials();
    if(creds != null) {
      builder.withAWSCredentialsProvider(new StaticCredentialsProvider(creds));
    }
    return builder;
  }

  @Override
  public boolean connectionEquals(VendorImplementationBase arg0) {
    return false;
  }

  public String getRegion() {
    return region;
  }

  /**
   * The Amazon Web Services region to use
   * 
   * @param str the region
   */
  public void setRegion(String str) {
    this.region = Args.notBlank(str, "region");
  }

  @Deprecated
  public String getAccessKey() {
    return accessKey;
  }

  /**
   * Your Amazon Web Services access key. This can be a root key or the key for an IAM user (recommended).
   * 
   * @param key the Access key.
   */
  @Deprecated
  public void setAccessKey(String key) {
    this.accessKey = Args.notBlank(key, "access-key");
  }

  @Deprecated
  public String getSecretKey() {
    return secretKey;
  }

  /**
   * Your Amazon Web Services secret key. This can be a root key or the key for an IAM user (recommended).
   * 
   * @param key the secret key which could encoded by {@linkplain Password#encode(String, String)}
   */
  @Deprecated
  public void setSecretKey(String key) {
    this.secretKey = Args.notBlank(key, "secret-key");
  }

  public Integer getPrefetchCount() {
    return prefetchCount;
  }

  /**
   * The maximum number of messages to retrieve from the Amazon SQS queue per request. When omitted
   * the default setting on the queue will be used.
   * 
   * @param prefetchCount
   */
  public void setPrefetchCount(Integer prefetchCount) {
    this.prefetchCount = prefetchCount;
  }

  int prefetchCount() {
    return getPrefetchCount() != null ? getPrefetchCount().intValue() : DEFAULT_PREFETCH_COUNT;
  }

  @Override
  public void prepare() throws CoreException {
    super.prepare();
  }
  
  protected void rethrowJMSException(String msg, Exception e) throws JMSException {
    JMSException jmse = new JMSException(msg);
    jmse.initCause(e);
    throw jmse;
  }

  protected void rethrowJMSException(Exception e) throws JMSException {
    rethrowJMSException(e.getMessage(), e);
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
