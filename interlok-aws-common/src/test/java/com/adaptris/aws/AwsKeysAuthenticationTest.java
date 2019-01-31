package com.adaptris.aws;

import static org.junit.Assert.assertNotNull;
import org.junit.Test;

public class AwsKeysAuthenticationTest {

  @Test
  public void testAWSCredentials() throws Exception {
    AWSKeysAuthentication auth = new AWSKeysAuthentication("accessKey", "secretKey");
    assertNotNull(auth.getAWSCredentials());
  }
}
