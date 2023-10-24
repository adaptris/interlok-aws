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

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.ClientConfigurationBuilder;
import com.adaptris.aws2.EndpointBuilder;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.util.KeyValuePairSet;
import org.junit.Test;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertNotNull;

public class SyncClientFactoryTest {

  @Test
  public void testCreateClient() throws Exception {
    SyncSQSClientFactory fac = new SyncSQSClientFactory();
    StaticCredentialsBuilder creds = new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey"));
    assertNotNull(
        fac.createClient(creds.build(), ClientConfigurationBuilder.build(new KeyValuePairSet()), new EndpointBuilder() {
          @Override
          public <T extends AwsClientBuilder<?, ?>> T rebuild(T builder) {
            builder.region(Region.AP_NORTHEAST_1);
            return builder;
          }
        }));
  }
}
