package com.adaptris.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import com.adaptris.aws.ClientConfigurationBuilder.ClientConfigurationProperties;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

public class ClientConfigurationPropertiesTest {

  @Test
  public void testConnectionTimeout() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.ConnectionTimeout.configure(new ClientConfiguration(), "1");
    assertEquals(1, cc.getConnectionTimeout());
  }

  @Test
  public void testConnectionTTL() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.ConnectionTTL.configure(new ClientConfiguration(), "1");
    assertEquals(1, cc.getConnectionTTL());
  }

  @Test
  public void testGzip() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.Gzip.configure(new ClientConfiguration(), "true");
    assertTrue(cc.useGzip());
  }

  @Test
  public void testLocalAddress() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.LocalAddress
        .configure(new ClientConfiguration(), "localhost");
    assertNotNull(cc.getLocalAddress());
  }

  @Test
  public void testMaxConnections() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.MaxConnections.configure(new ClientConfiguration(), "1");
    assertEquals(1, cc.getMaxConnections());
  }

  @Test
  public void testMaxErrorRetry() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.MaxErrorRetry.configure(new ClientConfiguration(), "1");
    assertEquals(1, cc.getMaxErrorRetry());
  }

  @Test
  public void testPreemptiveProxy() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.PreemptiveBasicProxyAuth
        .configure(new ClientConfiguration(), "true");
    assertTrue(cc.isPreemptiveBasicProxyAuth());
  }


  @Test
  public void testProtocol() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.Protocol.configure(new ClientConfiguration(), "HTTP");
    assertEquals(Protocol.HTTP, cc.getProtocol());
  }

  @Test
  public void testProxyDomain() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.ProxyDomain.configure(new ClientConfiguration(), "localhost");
    assertEquals("localhost", cc.getProxyDomain());
  }

  @Test
  public void testProxyHost() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.ProxyHost.configure(new ClientConfiguration(), "localhost");
    assertEquals("localhost", cc.getProxyHost());
    cc = ClientConfigurationProperties.ProxyHost.configure(new ClientConfiguration(), "");
    assertNull(cc.getProxyHost());
  }

  @Test
  public void testProxyPassword() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.ProxyPassword
        .configure(new ClientConfiguration(), "localhost");
    assertEquals("localhost", cc.getProxyPassword());
  }

  @Test
  public void testProxyPort() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.ProxyPort.configure(new ClientConfiguration(), "3128");
    assertEquals(3128, cc.getProxyPort());
    cc = ClientConfigurationProperties.ProxyPort.configure(new ClientConfiguration(), "");
    assertEquals(-1, cc.getProxyPort());
  }

  @Test
  public void testProxyUserName() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.ProxyUsername
        .configure(new ClientConfiguration(), "myUsername");
    assertEquals("myUsername", cc.getProxyUsername());
  }

  @Test
  public void testProxyWorkstation() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.ProxyWorkstation
        .configure(new ClientConfiguration(), "ProxyWorkstation");
    assertEquals("ProxyWorkstation", cc.getProxyWorkstation());
  }

  @Test
  public void testResponseMetadataCacheSize() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.ResponseMetadataCacheSize
        .configure(new ClientConfiguration(), "1");
    assertEquals(1, cc.getResponseMetadataCacheSize());
  }

  @Test
  public void testReaper() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.Reaper
            .configure(new ClientConfiguration(), "yes");
    assertEquals(true, cc.useReaper());
  }

  @Test
  public void testSignerOverride() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.SignerOverride.configure(new ClientConfiguration(),
            "MD5");
    assertEquals("MD5", cc.getSignerOverride());
  }

  @Test
  public void testSocketBufferSizeHints() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.SocketBufferSizeHints.configure(new ClientConfiguration(),
            "8192,8192");
    assertNotNull(cc.getSocketBufferSizeHints());
  }


  @Test
  public void testSocketTimeout() throws Exception {
    ClientConfiguration cc = ClientConfigurationProperties.SocketTimeout
        .configure(new ClientConfiguration(), "8192");
    assertEquals(8192, cc.getSocketTimeout());
  }


  @Test
  public void testTcpKeepAlive() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.TcpKeepAlive.configure(new ClientConfiguration(), "yes");
    assertTrue(cc.useTcpKeepAlive());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testUserAgent() throws Exception {
    ClientConfiguration cc =
        ClientConfigurationProperties.UserAgent.configure(new ClientConfiguration(), "java");
    assertEquals("java", cc.getUserAgent());
  }
}
