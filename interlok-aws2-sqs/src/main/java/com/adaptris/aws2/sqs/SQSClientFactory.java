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

package com.adaptris.aws2.sqs;

import com.adaptris.aws2.EndpointBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@FunctionalInterface
public interface SQSClientFactory {

  /**
   * Create a new Amazon SQS client.
   * 
   * @param creds The credentials to use. If null the default credentials mechanism for the Amazon AWS SDK will be used.
   * @param conf the ClientConfiguration to use.
   * @param endpointBuilder configures the endpoint / region that the client will use.
   * @return a {@link SqsAsyncClient} instance.
   */
  SqsAsyncClient createClient(AwsCredentialsProvider creds, ClientOverrideConfiguration conf, EndpointBuilder endpointBuilder);
  
}
