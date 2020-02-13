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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConsumerCase;
import com.adaptris.core.QuartzCronPoller;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.stubs.MessageCounter;
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
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class AwsConsumerTest extends ConsumerCase {

  private static final String payload = "The quick brown fox jumps over the lazy dog.";
  
  private AmazonSQS sqsClientMock;
  private AmazonSQSConnection connectionMock;
  
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
  
  @Test
  public void testConsumerInitialisation() throws Exception {
    StandaloneConsumer consumer = startConsumer();
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testWithoutQueueOwnerAWSAccountId() throws Exception {
    StandaloneConsumer consumer = startConsumer();
    verify(sqsClientMock, atLeast(1)).getQueueUrl(new GetQueueUrlRequest("queue"));
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testWithQueueOwnerAWSAccountId() throws Exception {
    StandaloneConsumer consumer = startConsumerWithAccountID();
    GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest("queue");
    getQueueUrlRequest.withQueueOwnerAWSAccountId("accountId");
    verify(sqsClientMock, atLeast(1)).getQueueUrl(getQueueUrlRequest);
    LifecycleHelper.stopAndClose(consumer);
  }
  
  @Test
  public void testSingleConsume_AlwaysDelete() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) anyObject())).thenReturn(createReceiveMessageResult(1),
        new ReceiveMessageResult());

    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);

    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testSingleConsume_DeleteOnSuccess() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1),
        new ReceiveMessageResult());

    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setAlwaysDelete(Boolean.FALSE);
    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(consumer);

    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);
    
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testSingleConsume_NoDeleteOnFailure() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) anyObject())).thenReturn(createReceiveMessageResult(1),
        new ReceiveMessageResult());

    NoCallbackListener messageListener = new NoCallbackListener();
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setAlwaysDelete(Boolean.FALSE);
    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(consumer);

    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);

    verify(sqsClientMock, never()).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
  }

  
  @Test
  public void testConsumeWithAmazonReceiveException() throws Exception {
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenThrow(new AmazonServiceException("expected"));

    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 0, 1100);
    verify(sqsClientMock, atLeast(0)).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
  }
  
  @Test
  public void testConsumeWithAmazonDeleteException() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1),
        new ReceiveMessageResult());
    
    doThrow(new AmazonServiceException("expected")).when(sqsClientMock).deleteMessage((DeleteMessageRequest)anyObject());
        
    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 3000);
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
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

    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setPrefetchCount(1);
    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(consumer);

    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);
    
    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    assertEquals("myValue", messageListener.getMessages().get(0).getMetadataValue("myKey"));
    assertEquals("myValue2", messageListener.getMessages().get(0).getMetadataValue("myKey2"));
    LifecycleHelper.stopAndClose(consumer);
  }
  
  @Test
  public void testSingleConsumeWithMessageAttributes() throws Exception {
    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("myKey", "myValue");
    attributes.put("myKey2", "myValue2");
    HashMap<String, String> msgAttributes = new HashMap<>();
    msgAttributes.put("myMsgAttribute", "myMsgAttributeValue");
    msgAttributes.put("myMsgAttribute2", "myMsgAttributeValue2");

    
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        createReceiveMessageResult(1, attributes, convertToMessageAttributes(msgAttributes)),
        new ReceiveMessageResult());
    
    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setPrefetchCount(1);
    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(consumer);

    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);
    
    assertEquals(1, messageListener.getMessages().size());
    assertEquals("myValue", messageListener.getMessages().get(0).getMetadataValue("myKey"));
    assertEquals("myValue2", messageListener.getMessages().get(0).getMetadataValue("myKey2"));
    assertEquals("myMsgAttributeValue", messageListener.getMessages().get(0).getMetadataValue("myMsgAttribute"));
    assertEquals("myMsgAttributeValue2", messageListener.getMessages().get(0).getMetadataValue("myMsgAttribute2"));
  }

  @Test
  public void testSingleConsumeWithMessageId() throws Exception {
    ReceiveMessageResult receiveMessageResult = createReceiveMessageResult(1);
    String expected = receiveMessageResult.getMessages().get(0).getMessageId();
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest)anyObject())).thenReturn(
        receiveMessageResult, new ReceiveMessageResult());

    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setPrefetchCount(1);
    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(consumer);

    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);

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
    
    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), numMsgs, 30000);
    Thread.sleep(500);
    
    verify(sqsClientMock, atLeast(numMsgs)).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testMessagesRemaining() throws Exception {
    when(sqsClientMock.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
        new GetQueueAttributesResult().addAttributesEntry(QueueAttributeName.ApproximateNumberOfMessages.toString(), "10")
    );
    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    int messages = sqsConsumer.messagesRemaining();

    assertEquals(10, messages);
    verify(sqsClientMock, atLeast(1)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }

  @Test
  public void testMessagesRemainingWithoutConsumerStart() throws Exception {
    when(sqsClientMock.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
        new GetQueueAttributesResult().addAttributesEntry(QueueAttributeName.ApproximateNumberOfMessages.toString(), "10")
    );
    MockMessageListener messageListener = new MockMessageListener(10);

    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);

    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
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

  private ReceiveMessageResult createReceiveMessageResult(int numMsgs, Map<String, String> attributes,
      Map<String, MessageAttributeValue> msgAttributes) {
    // Create the messages to be received
    GuidGenerator guidGenerator = new GuidGenerator();
    List<Message> msgs = new ArrayList<Message>();
    for (int i = 0; i < numMsgs; i++) {
      msgs.add(new Message().withBody(payload).withAttributes(attributes)
          .withMessageAttributes(msgAttributes)
          .withMessageId(guidGenerator.getUUID()));
    }
    // Set up the connection mock to return a message list when called
    ReceiveMessageResult result = new ReceiveMessageResult();
    result.setMessages(msgs);
    return result;
  }

  private Map<String, MessageAttributeValue> convertToMessageAttributes(Map<String,String> map) {
    Map<String, MessageAttributeValue> result = new HashMap<>();
    for (Entry<String, String> e : map.entrySet()) {
      result.put(e.getKey(), new MessageAttributeValue().withDataType("String").withStringValue(e.getValue()));
    }
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

  private void waitForConsumer(MessageCounter counter, final int numMsgs, final int maxWaitTime) throws Exception {
    final int waitInc = 100;
    int waitTime = 0;
    do {
      Thread.sleep(waitInc);
      waitTime += waitInc;
    } while (counter.messageCount() < numMsgs && waitTime < maxWaitTime);

    assertEquals(numMsgs, counter.messageCount());
  }

  private static AmazonSQSConsumer createConsumer(AmazonSQSConnection conn) {
    AmazonSQSConsumer sqsConsumer = new AmazonSQSConsumer();
    sqsConsumer.setDestination(new ConfiguredConsumeDestination("queue"));
    sqsConsumer.setPoller(new QuartzCronPoller("*/1 * * * * ?"));
    sqsConsumer.setReacquireLockBetweenMessages(true);
    sqsConsumer.registerConnection(conn);
    return sqsConsumer;
  }

  private StandaloneConsumer startConsumer() throws Exception {
    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    StandaloneConsumer standaloneConsumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    standaloneConsumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(standaloneConsumer);
    return standaloneConsumer;
  }

  private StandaloneConsumer startConsumerWithAccountID() throws Exception {
    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setOwnerAwsAccountId("accountId");

    StandaloneConsumer standaloneConsumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    standaloneConsumer.registerAdaptrisMessageListener(messageListener);

    LifecycleHelper.initAndStart(standaloneConsumer);
    return standaloneConsumer;
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

  private class NoCallbackListener extends MockMessageListener {
    public NoCallbackListener() {
    }

    // Fudge it so that we never trigger the callback, which "implies" an error
    @Override
    public void onAdaptrisMessage(AdaptrisMessage msg, Consumer<AdaptrisMessage> success) {
      super.onAdaptrisMessage(msg, (m) -> {
      });
    }

  }
}
