package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class DefaultAwsAuthenticationTest {

  @Test
  public void testAWSCredentials() throws Exception {
    DefaultAWSAuthentication auth = new DefaultAWSAuthentication();
    assertNull(auth.getAWSCredentials());
  }
}
