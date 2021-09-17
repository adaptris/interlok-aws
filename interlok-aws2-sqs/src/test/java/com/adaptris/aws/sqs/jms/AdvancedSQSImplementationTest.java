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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class AdvancedSQSImplementationTest extends AmazonSQSImplementationTest {

  @Test
  public void testConnectionFactory_WithClientConfiguration() throws Exception {
    AdvancedSQSImplementation jmsImpl = createImpl().withCredentialsProviderBuilder(
        new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("MyAccessKey", "MyKey")));
    jmsImpl.setRegion("eu-west-1");

    KeyValuePairSet kvps = jmsImpl.getClientConfigurationProperties();
    kvps.add(new KeyValuePair("ConnectionTimeout", "10"));
    kvps.add(new KeyValuePair("ConnectionTTL", "10"));
    kvps.add(new KeyValuePair("Gzip", "true"));
    kvps.add(new KeyValuePair("NonProxyHosts", "localhost"));

    kvps.add(new KeyValuePair("Hello World", "MyUserAgent"));
    jmsImpl.prepare();
    assertNotNull(jmsImpl.createConnectionFactory());
  }
  
  @Test
  public void testConnectionFactory_WithRetryPolicy() throws Exception {
    AdvancedSQSImplementation jmsImpl = createImpl();
    jmsImpl.setRegion("eu-west-1");
    jmsImpl.setRetryPolicy(new DefaultRetryPolicyFactory());
    jmsImpl.prepare();
    assertNotNull(jmsImpl.createConnectionFactory());
  }
  
  @Test
  public void testEndpointBuilder() {
    AdvancedSQSImplementation jmsImpl = createImpl();
    assertNull(jmsImpl.getCustomEndpoint());
    jmsImpl.setRegion("us-west-1");
    assertNotNull(jmsImpl.endpointBuilder());
    jmsImpl.withCustomEndpoint(new CustomEndpoint().withServiceEndpoint("http://localhost").withSigningRegion("us-west-1"));
    assertNotNull(jmsImpl.getCustomEndpoint());
    assertEquals(CustomEndpoint.class, jmsImpl.endpointBuilder().getClass());
    jmsImpl.withCustomEndpoint(new CustomEndpoint());
    assertNotNull(jmsImpl.getCustomEndpoint());
    assertNotSame(CustomEndpoint.class, jmsImpl.endpointBuilder().getClass());
  }


  @Override
  protected AdvancedSQSImplementation createImpl() {
    return new AdvancedSQSImplementation();
  }
}
