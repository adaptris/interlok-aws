/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws2.sqs;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.util.Closer;
import com.amazonaws.regions.Regions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class AmazonSQSConnectionTest {

  private AmazonSQSConnection amazonSQSConnection;

  @Mock
  private SqsAsyncClient mockSqsClient;
  @Mock
  private AsyncSQSClientFactory mockClientFactory;

  private AutoCloseable mocking;

  @Before
  public void setUp() throws Exception {
    amazonSQSConnection = new AmazonSQSConnection();

    mocking = MockitoAnnotations.openMocks(this);

    amazonSQSConnection.setSqsClientFactory(mockClientFactory);
    AWSKeysAuthentication auth = new AWSKeysAuthentication();
    auth.setAccessKey("accessKey");
    auth.setSecretKey("secretKey");
    amazonSQSConnection.setCredentials(StaticCredentialsProvider.create(auth.getAWSCredentials()));
    amazonSQSConnection.setRegion(Regions.AP_NORTHEAST_1.getName());
  }

  @After
  public void tearDown() throws Exception {
    amazonSQSConnection.stop();
    amazonSQSConnection.close();
    Closer.closeQuietly(mocking);
  }

  @Test
  public void testInit() throws Exception {
    when(mockClientFactory.createClient(any(), any(), any())).thenReturn(mockSqsClient);
    amazonSQSConnection.init();
  }


  @Test
  public void testStartUp() throws Exception {
    when(mockClientFactory.createClient(any(), any(), any())).thenReturn(mockSqsClient);
    amazonSQSConnection.init();
    amazonSQSConnection.start();
  }

  @Test
  public void testGetNullSyncClient() throws Exception {
    try {
      amazonSQSConnection.getSyncClient();
      fail("It's null should throw an exception.");
    } catch (CoreException ex) {
      //expected
    }
  }

  @Test
  public void testGetNullASyncClient() throws Exception {
    try {
      amazonSQSConnection.getASyncClient();
      fail("It's null should throw an exception.");
    } catch (CoreException ex) {
      //expected
    }
  }

  @Test
  public void testGetSyncClientAfterInit() throws Exception {
    when(mockClientFactory.createClient(any(), any(), any())).thenReturn(mockSqsClient);

    amazonSQSConnection.init();
    assertEquals(mockSqsClient, amazonSQSConnection.getSyncClient());
  }

  @Test
  public void testGetASyncClientAfterInit() throws Exception {
    when(mockClientFactory.createClient(any(), any(), any())).thenReturn(mockSqsClient);

    amazonSQSConnection.init();
    assertEquals(mockSqsClient, amazonSQSConnection.getASyncClient());
  }
}
