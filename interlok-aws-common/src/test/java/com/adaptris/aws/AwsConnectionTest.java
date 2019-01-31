package com.adaptris.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adaptris.core.CoreException;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

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
    setCustomEndpoint(new CustomEndpoint().withServiceEndpoint("http://localhost").withSigningRegion("us-west-1"));
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
