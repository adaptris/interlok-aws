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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;

public class AwsHelperTest extends AwsHelper {
  private static final String QUEUE_URL = "https://localhost/myqueueName";
  private AmazonSQS sqsClientMock;

  @Before
  public void setUp() throws Exception {

    sqsClientMock = mock(AmazonSQS.class);
    GetQueueUrlResult queueUrlResultMock = mock(GetQueueUrlResult.class);
    when(queueUrlResultMock.getQueueUrl()).thenReturn(QUEUE_URL);

    when(sqsClientMock.getQueueUrl((GetQueueUrlRequest)any())).thenReturn(queueUrlResultMock);
  }

  @Test
  public void testGetRegion() throws Exception {
    assertEquals(Regions.EU_WEST_1.getName(), formatRegion("eu-west-1"));
    assertEquals(Regions.EU_WEST_1.getName(), formatRegion("EU-WEST-1"));
    assertEquals(Regions.EU_WEST_1.getName(), formatRegion("amazon.eu-west-1"));
    assertEquals("blah", formatRegion("blah"));
  }

  @Test
  public void testBuildQueueUrl() throws Exception {
    assertEquals("https://localhost/anotherQueue", buildQueueUrl("https://localhost/anotherQueue", "", sqsClientMock));
    assertEquals(QUEUE_URL, buildQueueUrl("myQueueName", "", sqsClientMock));
    assertEquals(QUEUE_URL, buildQueueUrl("myQueueName", "ownerAccount", sqsClientMock));
  }
}
