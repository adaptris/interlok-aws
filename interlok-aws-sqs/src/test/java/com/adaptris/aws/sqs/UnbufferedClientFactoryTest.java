package com.adaptris.aws.sqs;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.adaptris.aws.ClientConfigurationBuilder;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

public class UnbufferedClientFactoryTest {

  @Test
  public void testCreateClient() throws Exception {
    UnbufferedSQSClientFactory fac = new UnbufferedSQSClientFactory();
    BasicAWSCredentials creds = new BasicAWSCredentials("accessKey", "secretKey");
    assertNotNull(
        fac.createClient(creds, ClientConfigurationBuilder.build(new KeyValuePairSet()), Regions.AP_NORTHEAST_1.getName()));
  }
}
