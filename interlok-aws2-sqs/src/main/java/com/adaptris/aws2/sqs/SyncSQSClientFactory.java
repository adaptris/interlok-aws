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
import com.thoughtworks.xstream.annotations.XStreamAlias;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

/**
 *
 * Unbuffered SQS Client Factory.
 *
 * @config unbuffered-sqs-client-factory
 * @since 4.3.0
 */
@XStreamAlias("aws2-sync-sqs-client-factory")
public class SyncSQSClientFactory implements SQSClientFactory {

  @Override
  public SqsClient createClient(AwsCredentialsProvider creds, ClientOverrideConfiguration conf, EndpointBuilder endpoint) {
    SqsClientBuilder builder = endpoint.rebuild(SqsClient.builder());

    builder.credentialsProvider(creds);
    builder.overrideConfiguration(conf);

    return builder.build();
  }
}
