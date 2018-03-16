package com.adaptris.aws;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.exc.AdaptrisSecurityException;
import com.adaptris.security.password.Password;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Specify explicit keys for AWS access. Either the root keys for the AWS account (not recommended) or IAM keys.
 */
@XStreamAlias("aws-keys-authentication")
@ComponentProfile(summary="Specify explicit keys for AWS access. Either the root keys for the AWS account (not recommended) or IAM keys.")
public class AWSKeysAuthentication implements AWSAuthentication {

  @NotNull
  private String accessKey;
  
  @NotNull
  @InputFieldHint(style = "PASSWORD", external = true)
  private String secretKey;

  public AWSKeysAuthentication() {

  }

  public AWSKeysAuthentication(String accesskey, String secretKey) {
    this();
    setAccessKey(accesskey);
    setSecretKey(secretKey);
  }

  @Override
  public AWSCredentials getAWSCredentials() throws AdaptrisSecurityException {
    return new BasicAWSCredentials(getAccessKey(), Password.decode(ExternalResolver.resolve(getSecretKey())));
  }
  
  /**
   * The access key for the AWS account
   * 
   * @return accessKey
   */
  public String getAccessKey() {
    return accessKey;
  }

  /**
   * The access key for the AWS account
   * 
   * @param accessKey
   */
  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  /**
   * The secret key for the AWS account. Can be encoded.
   * 
   * @return secretKey
   */
  public String getSecretKey() {
    return secretKey;
  }

  /**
   * The secret key for the AWS account. Can be encoded.
   * 
   * @param secretKey
   */
  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

}
