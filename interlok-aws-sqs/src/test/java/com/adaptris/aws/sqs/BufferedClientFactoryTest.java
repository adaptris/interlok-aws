package com.adaptris.aws.sqs;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.adaptris.aws.sqs.BufferedSQSClientFactory;
import com.amazonaws.auth.BasicAWSCredentials;

public class BufferedClientFactoryTest {

  @Test
  public void testCreateClient() throws Exception {
    BufferedSQSClientFactory fac = new BufferedSQSClientFactory();
    BasicAWSCredentials creds = new BasicAWSCredentials("accessKey", "secretKey");
    assertNotNull(fac.createClient(creds));
  }
}
