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

package com.adaptris.aws;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.exc.AdaptrisSecurityException;
import com.adaptris.security.password.Password;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

/**
 * Specify explicit keys for AWS access. Either the root keys for the AWS account (not recommended) or IAM keys.
 * 
 * <p>
 * In the event that both the secret and access key are left blank; then this will not create any credentials which will cause the
 * underlying system to default to a default AWS authentication chain
 * </p>
 */
@XStreamAlias("aws-keys-authentication")
@ComponentProfile(summary="Specify explicit keys for AWS access. Either the root keys for the AWS account (not recommended) or IAM keys.")
public class AWSKeysAuthentication implements AWSAuthentication {

  /**
   * The access key for the AWS account
   */
  @Getter
  @Setter
  private String accessKey;
  
  /**
   * The secret key for the AWS account which may be encoded or external.
   * 
   */
  @InputFieldHint(style = "PASSWORD", external = true)
  @Getter
  @Setter
  private String secretKey;

  public AWSKeysAuthentication() {

  }

  public AWSKeysAuthentication(String accesskey, String secretKey) {
    this();
    setAccessKey(accesskey);
    setSecretKey(secretKey);
  }

  @Override
  public AwsCredentials getAWSCredentials() throws AdaptrisSecurityException {
    if (StringUtils.isBlank(getAccessKey()) && StringUtils.isEmpty(getSecretKey())) {
      return null;
    }
    return AwsBasicCredentials.create(getAccessKey(), Password.decode(ExternalResolver.resolve(getSecretKey())));
  }

}
