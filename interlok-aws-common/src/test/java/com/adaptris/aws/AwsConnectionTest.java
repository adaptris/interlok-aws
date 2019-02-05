package com.adaptris.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adaptris.core.CoreException;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.client.builder.AwsClientBuilder;

public class AwsConnectionTest extends AWSConnection {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testAuthentication() {
    assertNull(getAuthentication());
    assertNotNull(authentication());
    assertEquals(DefaultAWSAuthentication.class, authentication().getClass());
    setAuthentication(new AWSKeysAuthentication("accessKey", "secretKey"));
    assertNotNull(getAuthentication());
    assertEquals(AWSKeysAuthentication.class, authentication().getClass());    
  }

  @Test
  public void testClientConfiguration() {
    assertNull(getClientConfiguration());
    assertEquals(0, clientConfiguration().size());
    KeyValuePairSet set = new KeyValuePairSet();
    set.add(new KeyValuePair("key", "value"));
    setClientConfiguration(set);
    assertEquals(set, getClientConfiguration());
    assertEquals(1, clientConfiguration().size());
  }

  @Test
  public void testRetryPolicy() {
    assertNull(getRetryPolicy());
    assertNotNull(retryPolicy());
    assertEquals(DefaultRetryPolicyFactory.class, retryPolicy().getClass());
    setRetryPolicy(new RetryNotFoundPolicyFactory());
    assertNotNull(getRetryPolicy());
    assertEquals(RetryNotFoundPolicyFactory.class, retryPolicy().getClass());    
  }

  @Test
  public void testEndpointBuilder() {
    assertNull(getCustomEndpoint());
    setRegion("us-west-1");
    assertNotNull(endpointBuilder());
    assertEquals(RegionOnly.class, endpointBuilder().getClass());
    withCustomEndpoint(new CustomEndpoint().withServiceEndpoint("http://localhost").withSigningRegion("us-west-1"));
    assertNotNull(getCustomEndpoint());
    assertEquals(CustomEndpoint.class, endpointBuilder().getClass());
  }
  

  @Test
  public void testRegion() {
    assertNull(getRegion());
    setRegion("us-west-1");
    assertEquals("us-west-1", getRegion());
    assertNotNull(endpointBuilder());
    assertEquals(RegionOnly.class, endpointBuilder().getClass());
  }
  
  @Test
  public void testRegionOnlyEndpointBuilder() {
    assertNull(getRegion());
    EndpointBuilder b0 = endpointBuilder();
    assertEquals(RegionOnly.class, b0.getClass());
    assertNull(b0.rebuild((AwsClientBuilder) new MockAwsClientBuilder()).getRegion());

    setRegion("us-west-1");
    assertEquals("us-west-1", getRegion());
    EndpointBuilder b1 = endpointBuilder();
    assertEquals(RegionOnly.class, b1.getClass());
    assertEquals("us-west-1", b1.rebuild((AwsClientBuilder)new MockAwsClientBuilder()).getRegion());

    setCustomEndpoint(new CustomEndpoint());
    EndpointBuilder b2 = endpointBuilder();
    assertEquals(RegionOnly.class, b2.getClass());
    assertEquals("us-west-1", b2.rebuild((AwsClientBuilder)new MockAwsClientBuilder()).getRegion());

    setCustomEndpoint(
        new CustomEndpoint().withServiceEndpoint("http:/127.0.0.1:4572")
            .withSigningRegion("us-east-1"));
    EndpointBuilder b3 = endpointBuilder();
    assertEquals(CustomEndpoint.class, b3.getClass());
    assertEquals("http:/127.0.0.1:4572",
        b3.rebuild((AwsClientBuilder)new MockAwsClientBuilder()).getEndpoint().getServiceEndpoint());

  }

  @Override
  protected void prepareConnection() throws CoreException {
  }

  @Override
  protected void initConnection() throws CoreException {
  }

  @Override
  protected void startConnection() throws CoreException {
  }

  @Override
  protected void stopConnection() {
  }

  @Override
  protected void closeConnection() {
  }

}
