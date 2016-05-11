package com.adaptris.core.aws.sqs;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;

public class UnbufferedClientFactoryTest {

  @Test
  public void testCreateClient() throws Exception {
    UnbufferedSQSClientFactory fac = new UnbufferedSQSClientFactory();
    BasicAWSCredentials creds = new BasicAWSCredentials("accessKey", "secretKey");
    assertNotNull(fac.createClient(creds));
  }
}
