package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;

public class AwsConnectionTest extends AWSConnection {

  @BeforeEach
  public void setUp() throws Exception {
  }

  @AfterEach
  public void tearDown() throws Exception {
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAuthentication() {
    assertNull(getCredentials());
    assertNotNull(credentialsProvider());
    assertEquals(StaticCredentialsBuilder.class, credentialsProvider().getClass());
    assertNull(getCredentials());
    assertEquals(StaticCredentialsBuilder.class, credentialsProvider().getClass());
  }

  @Test
  public void testClientConfiguration() {
    assertNull(getClientConfiguration());
    assertEquals(0, clientConfiguration().size());
    KeyValuePairSet set = new KeyValuePairSet();
    set.add(new KeyValuePair("key", "value"));
    withClientConfiguration(set);
    assertEquals(set, getClientConfiguration());
    assertEquals(1, clientConfiguration().size());
  }

  @Test
  public void testRetryPolicy() {
    assertNull(getRetryPolicy());
    assertNotNull(retryPolicy());
    assertEquals(DefaultRetryPolicyFactory.class, retryPolicy().getClass());
    withRetryPolicy(new RetryNotFoundPolicyFactory());
    assertNotNull(getRetryPolicy());
    assertEquals(RetryNotFoundPolicyFactory.class, retryPolicy().getClass());
  }

  @Test
  public void testEndpointBuilder() {
    assertNull(getCustomEndpoint());
    setRegion("us-west-1");
    assertNotNull(endpointBuilder());
    assertEquals(RegionEndpoint.class, endpointBuilder().getClass());
    withCustomEndpoint(new CustomEndpoint().withServiceEndpoint("http://localhost").withSigningRegion("us-west-1"));
    assertNotNull(getCustomEndpoint());
    assertEquals(CustomEndpoint.class, endpointBuilder().getClass());
  }


  @Test
  public void testRegion() {
    assertNull(getRegion());
    withRegion("us-west-1");
    assertEquals("us-west-1", getRegion());
    assertNotNull(endpointBuilder());
    assertEquals(RegionEndpoint.class, endpointBuilder().getClass());
  }

  @Test
  public void testRegionOnlyEndpointBuilder() {
    assertNull(getRegion());
    EndpointBuilder b0 = endpointBuilder();
    assertEquals(RegionEndpoint.class, b0.getClass());
    assertNull(((MockAwsClientBuilder)b0.rebuild((AwsClientBuilder) new MockAwsClientBuilder())).region);

    setRegion("us-west-1");
    assertEquals("us-west-1", getRegion());
    EndpointBuilder b1 = endpointBuilder();
    assertEquals(RegionEndpoint.class, b1.getClass());
    assertEquals("us-west-1", ((MockAwsClientBuilder)b1.rebuild((AwsClientBuilder) new MockAwsClientBuilder())).region.id());

    setCustomEndpoint(new CustomEndpoint());
    EndpointBuilder b2 = endpointBuilder();
    assertEquals(RegionEndpoint.class, b2.getClass());
    assertEquals("us-west-1", ((MockAwsClientBuilder)b2.rebuild((AwsClientBuilder) new MockAwsClientBuilder())).region.id());

    setCustomEndpoint(
        new CustomEndpoint().withServiceEndpoint("http:/127.0.0.1:4572")
            .withSigningRegion("us-east-1"));
    EndpointBuilder b3 = endpointBuilder();
    assertEquals(CustomEndpoint.class, b3.getClass());
    assertEquals("http:/127.0.0.1:4572", ((MockAwsClientBuilder)b3.rebuild((AwsClientBuilder) new MockAwsClientBuilder())).endpoint.toString());

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
