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

package com.adaptris.aws.sqs;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConsumerCase;
import com.adaptris.core.QuartzCronPoller;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.GuidGenerator;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class AwsConsumerTest extends ConsumerCase {

  private static final String payload = "The quick brown fox jumps over the lazy dog.";
  
  private MockMessageListener messageListener;
  
  private AmazonSQS sqsClientMock;
  private AmazonSQSConnection connectionMock;

  private AmazonSQSConsumer sqsConsumer;

  private StandaloneConsumer standaloneConsumer;
  
  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Before
  public void setUp() throws Exception {

    sqsClientMock = mock(AmazonSQS.class);
    GetQueueUrlResult queueUrlResultMock = mock(GetQueueUrlResult.class);
    when(sqsClientMock.getQueueUrl((GetQueueUrlRequest)anyObject())).thenReturn(queueUrlResultMock);

    // Initialize SQS Connection mock
    connectionMock = mock(AmazonSQSConnection.class);
    when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.getSyncClient()).thenReturn(sqsClientMock);
  }
  
  @After
  public void tearDown() throws Exception {
    LifecycleHelper.stopAndClose(sqsConsumer);
  }
  
  @Test
  public void testConsumerInitialisation() throws Exception {
    startConsumer();
  }

  @Test
  public void testWithoutQueueOwnerAWSAccountId() throws Exception {
    startConsumer();
    verify(sqsClientMock, atLeast(1)).getQueueUrl(new GetQueueUrlRequest("queue"));
  }

  @Test
  public void testWithQueueOwnerAWSAccountId() throws Exception {
    startConsumerWithAccountID();
    GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest("queue");
    getQueueUrlRequest.withQueueOwnerAWSAccountId("accountId");
    verify(sqsClientMock, atLeast(1)).getQueueUrl(getQueueUrlRequest);
  }
  
  @Test
  public void testSingleConsume() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1),
        new ReceiveMessageResult());

    startConsumer();
    waitForConsumer(1, 10000);
    
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
  }
  
  @Test
  public void testConsumeWithAmazonReceiveException() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenThrow(new AmazonServiceException("expected"));

    startConsumer();
    waitForConsumer(0, 0);
    verify(sqsClientMock, atLeast(0)).deleteMessage(any(DeleteMessageRequest.class));
  }
  
  @Test
  public void testConsumeWithAmazonDeleteException() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1),
        new ReceiveMessageResult());
    
    doThrow(new AmazonServiceException("expected")).when(sqsClientMock).deleteMessage((DeleteMessageRequest)anyObject());
        
    startConsumer();
    waitForConsumer(1, 3000);
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
  }
  
  @Test
  public void testSingleConsumeWithAddAtts() throws Exception {
    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("myKey", "myValue");
    attributes.put("myKey2", "myValue2");
    
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1, attributes),
        new ReceiveMessageResult());

    startConsumer();
    sqsConsumer.setPrefetchCount(1);
    waitForConsumer(1, 10000);
    
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    assertEquals(1, messageListener.getMessages().size());
    assertEquals("myValue", messageListener.getMessages().get(0).getMetadataValue("myKey"));
    assertEquals("myValue2", messageListener.getMessages().get(0).getMetadataValue("myKey2"));
  }
  
  @Test
  public void testSingleConsumeWithSetReceieveAtts() throws Exception {
    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("myKey", "myValue");
    attributes.put("myKey2", "myValue2");
    
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1, attributes),
        new ReceiveMessageResult());
    
    List<String> receiveAttributes = new ArrayList<>();
    receiveAttributes.add("myReceiveKey");
    receiveAttributes.add("myReceiveKey2");

    startConsumer();
    sqsConsumer.setPrefetchCount(1);
    waitForConsumer(1, 10000);
    
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    assertEquals(1, messageListener.getMessages().size());
    assertEquals("myValue", messageListener.getMessages().get(0).getMetadataValue("myKey"));
    assertEquals("myValue2", messageListener.getMessages().get(0).getMetadataValue("myKey2"));
  }

  @Test
  public void testSingleConsumeWithMessageId() throws Exception {
    ReceiveMessageResult receiveMessageResult = createReceiveMessageResult(1);
    String expected = receiveMessageResult.getMessages().get(0).getMessageId();
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        receiveMessageResult, new ReceiveMessageResult());

    startConsumer();
    sqsConsumer.setPrefetchCount(1);
    waitForConsumer(1, 10000);

    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    assertEquals(1, messageListener.getMessages().size());
    assertEquals(expected, messageListener.getMessages().get(0).getMetadataValue("SQSMessageID"));
  }
  
  
  @Test
  public void testMultipleConsume() throws Exception {
    final int b1 = 5, b2 = 5, b3 = 5, b4 = 5, b5 = 3;
    final int numMsgs = b1 + b2 + b3 + b4 + b5;

    // Return a ReceiveMessageResult the first calls, an empty result the last call
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(b1),
        createReceiveMessageResult(b2),
        createReceiveMessageResult(b3),
        createReceiveMessageResult(b4),
        createReceiveMessageResult(b5),
        new ReceiveMessageResult());
    
    startConsumer();
    waitForConsumer(numMsgs, 30000);
    Thread.sleep(500);
    
    verify(sqsClientMock, atLeast(numMsgs)).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testMessagesRemaining() throws Exception {
    when(sqsClientMock.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
        new GetQueueAttributesResult().addAttributesEntry(QueueAttributeName.ApproximateNumberOfMessages.toString(), "10")
    );
    startConsumer();
    int messages = sqsConsumer.messagesRemaining();

    assertEquals(10, messages);
    verify(sqsClientMock, atLeast(1)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }

  @Test
  public void testMessagesRemainingWithoutConsumerStart() throws Exception {
    when(sqsClientMock.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
        new GetQueueAttributesResult().addAttributesEntry(QueueAttributeName.ApproximateNumberOfMessages.toString(), "10")
    );
    messageListener = new MockMessageListener(10);

    sqsConsumer = new AmazonSQSConsumer();
    sqsConsumer.registerConnection(connectionMock);
    sqsConsumer.setDestination(new ConfiguredConsumeDestination("queue"));
    sqsConsumer.setPoller(new QuartzCronPoller("*/1 * * * * ?"));
    sqsConsumer.setReacquireLockBetweenMessages(true);

    standaloneConsumer = new StandaloneConsumer(sqsConsumer);
    standaloneConsumer.registerAdaptrisMessageListener(messageListener);
    int messages = sqsConsumer.messagesRemaining();

    assertEquals(10, messages);
    verify(sqsClientMock, atLeast(1)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }
  
  private ReceiveMessageResult createReceiveMessageResult(int numMsgs) {
    // Create the messages to be received
    GuidGenerator guidGenerator = new GuidGenerator();
    List<Message> msgs = new ArrayList<Message>();
    for(int i=0; i<numMsgs; i++) {
      msgs.add(new Message().withBody(payload).withMessageId(guidGenerator.getUUID()));
    }
    
    // Set up the connection mock to return a message list when called
    ReceiveMessageResult result = new ReceiveMessageResult();
    result.setMessages(msgs);
    
    return result;
  }
  
  private ReceiveMessageResult createReceiveMessageResult(int numMsgs, Map<String, String> attributes) {
    // Create the messages to be received
    GuidGenerator guidGenerator = new GuidGenerator();
    List<Message> msgs = new ArrayList<Message>();
    for(int i=0; i<numMsgs; i++) {
      msgs.add(new Message().withBody(payload).withAttributes(attributes).withMessageId(guidGenerator.getUUID()));
    }
    
    // Set up the connection mock to return a message list when called
    ReceiveMessageResult result = new ReceiveMessageResult();
    result.setMessages(msgs);
    
    return result;
  }

  private void waitForConsumer(final int numMsgs, final int maxWaitTime) throws Exception{
    final int waitInc = 100;
    int waitTime = 0;
    do {
      Thread.sleep(waitInc);
      waitTime += waitInc;
    } while(messageListener.messageCount() < numMsgs && waitTime < maxWaitTime);

    LifecycleHelper.stop(sqsConsumer);    
    assertEquals(numMsgs, messageListener.messageCount());
  }
  
  private void startConsumer() throws Exception {
    messageListener = new MockMessageListener(10);
    
    sqsConsumer = new AmazonSQSConsumer();
    sqsConsumer.registerConnection(connectionMock);
    sqsConsumer.setDestination(new ConfiguredConsumeDestination("queue"));
    sqsConsumer.setPoller(new QuartzCronPoller("*/1 * * * * ?"));
    sqsConsumer.setReacquireLockBetweenMessages(true);
    
    standaloneConsumer = new StandaloneConsumer(sqsConsumer);
    standaloneConsumer.registerAdaptrisMessageListener(messageListener);
    
    LifecycleHelper.init(sqsConsumer);
    LifecycleHelper.start(sqsConsumer);
  }

  private void startConsumerWithAccountID() throws Exception {
    messageListener = new MockMessageListener(10);

    sqsConsumer = new AmazonSQSConsumer();
    sqsConsumer.registerConnection(connectionMock);
    sqsConsumer.setDestination(new ConfiguredConsumeDestination("queue"));
    sqsConsumer.setPoller(new QuartzCronPoller("*/1 * * * * ?"));
    sqsConsumer.setReacquireLockBetweenMessages(true);
    sqsConsumer.setOwnerAwsAccountId("accountId");

    standaloneConsumer = new StandaloneConsumer(sqsConsumer);
    standaloneConsumer.registerAdaptrisMessageListener(messageListener);

    LifecycleHelper.init(sqsConsumer);
    LifecycleHelper.start(sqsConsumer);
  }
  
  @Override
  protected Object retrieveObjectForSampleConfig() {
    AmazonSQSConsumer sqsConsumer = new AmazonSQSConsumer();
    sqsConsumer.setDestination(new ConfiguredConsumeDestination("SampleQueue"));
    
    AmazonSQSConnection conn = new AmazonSQSConnection();
    conn.setCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
    conn.setRegion("My AWS Region");
    KeyValuePairSet clientSettings = new KeyValuePairSet();
    clientSettings.add(new KeyValuePair("ProxyHost", "127.0.0.1"));
    clientSettings.add(new KeyValuePair("ProxyPort", "3128"));
    conn.setClientConfiguration(clientSettings);
    sqsConsumer.setOwnerAwsAccountId("owner-account-id");
    StandaloneConsumer result = new StandaloneConsumer(conn, sqsConsumer);
    return result;
  }
}
