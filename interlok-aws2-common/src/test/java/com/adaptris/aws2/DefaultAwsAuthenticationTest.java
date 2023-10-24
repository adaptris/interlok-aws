package com.adaptris.aws2;

import org.junit.Test;

import static org.junit.Assert.assertNull;

public class DefaultAwsAuthenticationTest {

  @Test
  public void testAWSCredentials() throws Exception {
    DefaultAWSAuthentication auth = new DefaultAWSAuthentication();
    assertNull(auth.getAWSCredentials());
  }
}
