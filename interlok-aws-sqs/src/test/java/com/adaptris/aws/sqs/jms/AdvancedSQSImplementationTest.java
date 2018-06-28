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

package com.adaptris.aws.sqs.jms;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class AdvancedSQSImplementationTest extends AmazonSQSImplementationTest {

  @Test
  public void testConnectionFactory_WithClientConfiguration() throws Exception {
    AdvancedSQSImplementation jmsImpl = new AdvancedSQSImplementation();
    jmsImpl.setAuthentication(new AWSKeysAuthentication("MyAccessKey", "MyKey"));
    jmsImpl.setRegion("eu-west-1");
    
    KeyValuePairSet kvps = jmsImpl.getClientConfigurationProperties();
    kvps.add(new KeyValuePair("ConnectionTimeout", "10"));
    kvps.add(new KeyValuePair("ConnectionTTL", "10"));
    kvps.add(new KeyValuePair("Gzip", "true"));
    kvps.add(new KeyValuePair("NonProxyHosts", "localhost"));

    kvps.add(new KeyValuePair("Hello World", "MyUserAgent"));
    assertNotNull(jmsImpl.createConnectionFactory());
  }

  @Test
  public void testConnectionFactory_WithRetryPolicy() throws Exception {
    AdvancedSQSImplementation jmsImpl = new AdvancedSQSImplementation();
    jmsImpl.setRegion("eu-west-1");
    jmsImpl.setRetryPolicyBuilder(new RetryPolicyBuilder());
    assertNotNull(jmsImpl.createConnectionFactory());
  }
}
