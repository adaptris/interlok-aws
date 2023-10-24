package com.adaptris.aws2;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AwsKeysAuthenticationTest {

  @Test
  public void testAWSCredentials() throws Exception {
    assertNotNull(new AWSKeysAuthentication("accessKey", "secretKey").getAWSCredentials());
    assertNull(new AWSKeysAuthentication("", "secretKey").getAWSCredentials());
    assertNull(new AWSKeysAuthentication("accessKey", "").getAWSCredentials());
    assertNull(new AWSKeysAuthentication().getAWSCredentials());
  }
}
