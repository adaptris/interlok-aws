package com.adaptris.aws.sqs.jms;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.adaptris.aws.sqs.jms.AdvancedSQSImplementation;
import com.adaptris.aws.sqs.jms.RetryPolicyBuilder;
import com.adaptris.aws.sqs.jms.AdvancedSQSImplementation.ClientConfigurationProperties;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class AdvancedSQSImplementationTest extends AmazonSQSImplementationTest {

  @Test
  public void testConnectionFactory_WithClientConfiguration() throws Exception {
    AdvancedSQSImplementation jmsImpl = new AdvancedSQSImplementation();
    jmsImpl.setAccessKey("MyAccessKey");
    jmsImpl.setSecretKey("MyKey");
    jmsImpl.setRegion("eu-west-1");
    
    KeyValuePairSet kvps = jmsImpl.getClientConfigurationProperties();
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ConnectionTimeout.name(), "10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ConnectionTTL.name(), "10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.Gzip.name(), "true"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.LocalAddress.name(), "127.0.0.1"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.MaxConnections.name(), "10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.MaxErrorRetry.name(), "10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.PreemptiveBasicProxyAuth.name(), "true"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.Protocol.name(), "HTTPS"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ProxyDomain.name(), "ProxyDomain"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ProxyHost.name(), "127.0.0.1"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ProxyPassword.name(), "ProxyPassword"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ProxyUsername.name(), "ProxyUsername"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ProxyWorkstation.name(), "ProxyWorkstation"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ProxyPort.name(), "3306"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.Reaper.name(), "true"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.ResponseMetadataCacheSize.name(), "10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.SignerOverride.name(), "mySigner"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.SocketBufferSizeHints.name(), "10,10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.SocketTimeout.name(), "10"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.TcpKeepAlive.name(), "true"));
    kvps.add(new KeyValuePair(ClientConfigurationProperties.UserAgent.name(), "MyUserAgent"));
    kvps.add(new KeyValuePair("Hello World", "MyUserAgent"));
    assertNotNull(jmsImpl.createConnectionFactory());
  }

  @Test
  public void testConnectionFactory_WithRetryPolicy() throws Exception {
    AdvancedSQSImplementation jmsImpl = new AdvancedSQSImplementation();
    jmsImpl.setAccessKey("MyAccessKey");
    jmsImpl.setSecretKey("MyKey");
    jmsImpl.setRegion("eu-west-1");
    jmsImpl.setRetryPolicyBuilder(new RetryPolicyBuilder());
    assertNotNull(jmsImpl.createConnectionFactory());
  }
}
