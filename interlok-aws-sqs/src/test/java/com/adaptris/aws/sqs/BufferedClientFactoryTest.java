package com.adaptris.aws.sqs;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.auth.BasicAWSCredentials;

public class BufferedClientFactoryTest {

  @Test
  public void testCreateClient() throws Exception {
    BufferedSQSClientFactory fac = new BufferedSQSClientFactory();
    BasicAWSCredentials creds = new BasicAWSCredentials("accessKey", "secretKey");
    assertNotNull(fac.createClient(creds, ClientConfigurationBuilder.build(new KeyValuePairSet())));
  }
}
