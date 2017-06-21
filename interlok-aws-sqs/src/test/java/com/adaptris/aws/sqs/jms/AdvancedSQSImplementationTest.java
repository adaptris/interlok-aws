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
