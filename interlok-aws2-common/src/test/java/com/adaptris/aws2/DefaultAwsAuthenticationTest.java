package com.adaptris.aws2;

import static org.junit.Assert.assertNull;
import org.junit.Test;

public class DefaultAwsAuthenticationTest {

  @Test
  public void testAWSCredentials() throws Exception {
    DefaultAWSAuthentication auth = new DefaultAWSAuthentication();
    assertNull(auth.getAWSCredentials());
  }
}
