package com.adaptris.core.aws.sqs;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.security.exc.AdaptrisSecurityException;
import com.adaptris.security.password.Password;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("aws-keys-authentication")
public class AWSKeysAuthentication implements AWSAuthentication {

  @NotNull
  private String accessKey;
  
  @NotNull
  @InputFieldHint(style="PASSWORD")
  private String secretKey;
  
  @Override
  public AWSCredentials getAWSCredentials() throws AdaptrisSecurityException {
    return new BasicAWSCredentials(getAccessKey(), Password.decode(getSecretKey()));
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
