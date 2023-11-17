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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.aws.EndpointBuilder;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;

public class BufferedClientFactoryTest {

  @Test
  public void testCreateClient() throws Exception {
    BufferedSQSClientFactory fac = new BufferedSQSClientFactory();
    StaticCredentialsBuilder creds =
        new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey"));
    assertNotNull(
        fac.createClient(creds.build(), ClientConfigurationBuilder.build(new KeyValuePairSet()), new EndpointBuilder() {
          @Override
          public <T extends AwsClientBuilder<?, ?>> T rebuild(T builder) {
            builder.withRegion(Regions.AP_NORTHEAST_1.getName());
            return builder;
          }
        }));
  }
}
