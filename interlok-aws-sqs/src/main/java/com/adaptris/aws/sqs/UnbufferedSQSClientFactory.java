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

package com.adaptris.aws.sqs;

import static com.adaptris.aws.sqs.AwsHelper.formatRegion;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * 
 * Unbuffered SQS Client Factory.
 * 
 * @config unbuffered-sqs-client-factory
 * @since 3.0.3
 */
@XStreamAlias("unbuffered-sqs-client-factory")
public class UnbufferedSQSClientFactory implements SQSClientFactory {

  public UnbufferedSQSClientFactory() {
  }

  @Override
  public AmazonSQSAsync createClient(AWSCredentials creds, ClientConfiguration conf, String region) {
    AmazonSQSAsyncClientBuilder builder = AmazonSQSAsyncClientBuilder.standard().withClientConfiguration(conf)
        .withRegion(formatRegion(region));
    if (creds != null) {
      builder.withCredentials(new AWSStaticCredentialsProvider(creds));
    }
    return builder.build();
  }
  
}
