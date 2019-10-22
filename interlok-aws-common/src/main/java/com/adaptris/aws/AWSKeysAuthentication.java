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

import javax.validation.constraints.NotNull;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.exc.AdaptrisSecurityException;
import com.adaptris.security.password.Password;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Specify explicit keys for AWS access. Either the root keys for the AWS account (not recommended) or IAM keys.
 */
@XStreamAlias("aws-keys-authentication")
@ComponentProfile(summary="Specify explicit keys for AWS access. Either the root keys for the AWS account (not recommended) or IAM keys.")
public class AWSKeysAuthentication implements AWSAuthentication {

  /**
   * The access key for the AWS account
   */
  @NotNull
  @Getter
  @Setter
  @NonNull
  private String accessKey;
  
  /**
   * The secret key for the AWS account which may be encoded or external.
   * 
   */
  @NotNull
  @InputFieldHint(style = "PASSWORD", external = true)
  @Getter
  @Setter
  @NonNull
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

}
