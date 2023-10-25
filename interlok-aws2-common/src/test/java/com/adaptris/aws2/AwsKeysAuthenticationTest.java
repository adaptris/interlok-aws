package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class AwsKeysAuthenticationTest {

  @Test
  public void testAWSCredentials() throws Exception {
    assertNotNull(new AWSKeysAuthentication("accessKey", "secretKey").getAWSCredentials());
    assertNull(new AWSKeysAuthentication("", "secretKey").getAWSCredentials());
    assertNull(new AWSKeysAuthentication("accessKey", "").getAWSCredentials());
    assertNull(new AWSKeysAuthentication().getAWSCredentials());
  }
}
