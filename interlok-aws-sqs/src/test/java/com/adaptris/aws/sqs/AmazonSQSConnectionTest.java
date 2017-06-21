package com.adaptris.aws.sqs;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.core.CoreException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsync;

import junit.framework.TestCase;

public class AmazonSQSConnectionTest extends TestCase {
  
  private AmazonSQSConnection amazonSQSConnection;
  
  @Mock
  private AmazonSQSAsync mockSqsClient;
  @Mock
  private UnbufferedSQSClientFactory mockClientFactory;

  public void setUp() throws Exception {
    amazonSQSConnection = new AmazonSQSConnection();
    
    MockitoAnnotations.initMocks(this);
    
    amazonSQSConnection.setSqsClientFactory(mockClientFactory);
    AWSKeysAuthentication auth = new AWSKeysAuthentication();
    auth.setAccessKey("accessKey");
    auth.setSecretKey("secretKey");
    amazonSQSConnection.setAuthentication(auth);
    amazonSQSConnection.setRegion(Regions.AP_NORTHEAST_1.getName());
  }
  
  public void tearDown() throws Exception {
    amazonSQSConnection.stop();
    amazonSQSConnection.close();
  }

  public void testInit() throws Exception {
    when(mockClientFactory.createClient((AWSCredentials) anyObject(), (ClientConfiguration) anyObject(), anyString()))
        .thenReturn(mockSqsClient);
    amazonSQSConnection.init();
  }


  public void testStartUp() throws Exception {
    when(mockClientFactory.createClient((AWSCredentials) anyObject(), (ClientConfiguration) anyObject(), anyString()))
        .thenReturn(mockSqsClient);
    amazonSQSConnection.init();
    amazonSQSConnection.start();
  }

  public void testGetNullSyncClient() throws Exception {
    try {
      amazonSQSConnection.getSyncClient();
      fail("It's null should throw an exception.");
    } catch (CoreException ex) {
      //expected
    }
  }
  
  public void testGetNullASyncClient() throws Exception {
    try {
      amazonSQSConnection.getASyncClient();
      fail("It's null should throw an exception.");
    } catch (CoreException ex) {
      //expected
    }
  }
  
  public void testGetSyncClientAfterInit() throws Exception {
    when(mockClientFactory.createClient((AWSCredentials) anyObject(), (ClientConfiguration) anyObject(), anyString()))
        .thenReturn(mockSqsClient);
    
    amazonSQSConnection.init();
    assertEquals(mockSqsClient, amazonSQSConnection.getSyncClient());
  }
  
  public void testGetASyncClientAfterInit() throws Exception {
    when(mockClientFactory.createClient((AWSCredentials) anyObject(), (ClientConfiguration) anyObject(), anyString()))
        .thenReturn(mockSqsClient);
    
    amazonSQSConnection.init();
    assertEquals(mockSqsClient, amazonSQSConnection.getASyncClient());
  }
}
