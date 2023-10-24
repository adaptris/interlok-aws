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

package com.adaptris.aws2;

import com.adaptris.annotation.ComponentProfile;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

/**
 * Use the default authentication logic of the AWS SDK (IAM Roles, System Properties, Environment variables, etc
 *
 * @since 4.3.0
 */
@XStreamAlias("aws2-default-authentication")
@ComponentProfile(summary="Use the default authentication logic of the AWS SDK (IAM Roles, System Properties, Environment variables, etc", since = "4.3.0")
public class DefaultAWSAuthentication implements AWSAuthentication {

  @Override
  public AwsCredentials getAWSCredentials() {
    return null;
  }

}
