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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.QuartzCronPoller;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.stubs.MessageCounter;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.ExampleConsumerCase;
import com.adaptris.util.GuidGenerator;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class AwsConsumerTest extends ExampleConsumerCase {

  private static final String payload = "The quick brown fox jumps over the lazy dog.";

  private SqsClient sqsClientMock;
  private AmazonSQSConnection connectionMock;


  @BeforeEach
  public void setUp() throws Exception {

    sqsClientMock = mock(SqsClient.class);
    GetQueueUrlResponse queueUrlResultMock = mock(GetQueueUrlResponse.class);
    when(sqsClientMock.getQueueUrl((GetQueueUrlRequest) any())).thenReturn(queueUrlResultMock);

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
    verify(sqsClientMock, atLeast(1)).getQueueUrl(GetQueueUrlRequest.builder().queueName("queue").build());
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testWithQueueOwnerAWSAccountId() throws Exception {
    StandaloneConsumer consumer = startConsumerWithAccountID();
    GetQueueUrlRequest.Builder getQueueUrlRequest = GetQueueUrlRequest.builder().queueName("queue");
    getQueueUrlRequest.queueOwnerAWSAccountId("accountId");
    verify(sqsClientMock, atLeast(1)).getQueueUrl(getQueueUrlRequest.build());
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testSingleConsume_AlwaysDelete() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(1),
        createReceiveMessageResult(0));

    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);

    verify(sqsClientMock, atLeast(1)).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testSingleConsume_DeleteOnSuccess() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(1),
        ReceiveMessageResponse.builder().build());

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
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(1),
        ReceiveMessageResponse.builder().build());

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
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any()))
        .thenThrow(AwsServiceException.create("expected", new Exception()));

    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 0, 1100);
    verify(sqsClientMock, atLeast(0)).deleteMessage(any(DeleteMessageRequest.class));
    LifecycleHelper.stopAndClose(consumer);
  }

  @Test
  public void testConsumeWithAmazonDeleteException() throws Exception {
    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(1),
        ReceiveMessageResponse.builder().build());

    doThrow(AwsServiceException.create("expected", new Exception())).when(sqsClientMock)
        .deleteMessage((DeleteMessageRequest) any());

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
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(1, attributes),
        ReceiveMessageResponse.builder().build());

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
    attributes.put("myMsgAttribute", "myMsgAttributeValue");
    attributes.put("myMsgAttribute2", "myMsgAttributeValue2");

    // Return the ReceiveMessageResult with 1 message the first call, an empty result the second and all subsequent calls
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(1, attributes),
            ReceiveMessageResponse.builder().build());

    MockMessageListener messageListener = new MockMessageListener(10);
    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);
    sqsConsumer.setPrefetchCount(1);
    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    LifecycleHelper.initAndStart(consumer);

    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), 1, 10000);

    List<AdaptrisMessage> messages = messageListener.getMessages();
    assertEquals(1, messages.size());
    AdaptrisMessage message = messages.get(0);
    assertEquals("myValue", message.getMetadataValue("myKey"));
    assertEquals("myValue2", message.getMetadataValue("myKey2"));
    assertEquals("myMsgAttributeValue", message.getMetadataValue("myMsgAttribute"));
    assertEquals("myMsgAttributeValue2", message.getMetadataValue("myMsgAttribute2"));
  }

  @Test
  public void testSingleConsumeWithMessageId() throws Exception {
    ReceiveMessageResponse receiveMessageResult = createReceiveMessageResult(1);
    String expected = receiveMessageResult.messages().get(0).messageId();
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        receiveMessageResult, ReceiveMessageResponse.builder().build());

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
    when(sqsClientMock.receiveMessage((ReceiveMessageRequest) any())).thenReturn(
        createReceiveMessageResult(b1),
        createReceiveMessageResult(b2),
        createReceiveMessageResult(b3),
        createReceiveMessageResult(b4),
        createReceiveMessageResult(b5),
        ReceiveMessageResponse.builder().build());

    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    waitForConsumer((MessageCounter) sqsConsumer.retrieveAdaptrisMessageListener(), numMsgs, 30000);
    Thread.sleep(500);

    verify(sqsClientMock, atLeast(numMsgs)).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  public void testMessagesRemaining() throws Exception {

    Map<QueueAttributeName, String> attributes = new HashMap<>();
    attributes.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "10");

    when(sqsClientMock.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
            GetQueueAttributesResponse.builder().attributes(attributes).build());
    StandaloneConsumer consumer = startConsumer();
    AmazonSQSConsumer sqsConsumer = (AmazonSQSConsumer) consumer.getConsumer();
    int messages = sqsConsumer.messagesRemaining();

    assertEquals(10, messages);
    verify(sqsClientMock, atLeast(1)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }

  @Test
  public void testMessagesRemainingWithoutConsumerStart() throws Exception {

    Map<QueueAttributeName, String> attributes = new HashMap<>();
    attributes.put(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "10");

    when(sqsClientMock.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(
            GetQueueAttributesResponse.builder().attributes(attributes).build());
    MockMessageListener messageListener = new MockMessageListener(10);

    AmazonSQSConsumer sqsConsumer = createConsumer(connectionMock);

    StandaloneConsumer consumer = new StandaloneConsumer(connectionMock, sqsConsumer);
    consumer.registerAdaptrisMessageListener(messageListener);
    int messages = sqsConsumer.messagesRemaining();

    assertEquals(10, messages);
    verify(sqsClientMock, atLeast(1)).getQueueAttributes(any(GetQueueAttributesRequest.class));
  }

  private ReceiveMessageResponse createReceiveMessageResult(int numMsgs) {
    // Create the messages to be received
    GuidGenerator guidGenerator = new GuidGenerator();
    List<Message> msgs = new ArrayList<>();
    for(int i=0; i<numMsgs; i++) {
      msgs.add(Message.builder().body(payload).messageId(guidGenerator.getUUID()).build());
    }

    // Set up the connection mock to return a message list when called
    ReceiveMessageResponse.Builder result = ReceiveMessageResponse.builder();
    result.messages(msgs);
    return result.build();
  }

  private ReceiveMessageResponse createReceiveMessageResult(int numMsgs, Map<String, String> attributes) {
    // Create the messages to be received
    GuidGenerator guidGenerator = new GuidGenerator();
    List<Message> msgs = new ArrayList<Message>();
    for(int i=0; i<numMsgs; i++) {
      msgs.add(Message.builder().body(payload).attributesWithStrings(attributes).messageId(guidGenerator.getUUID()).build());
    }

    // Set up the connection mock to return a message list when called
    ReceiveMessageResponse.Builder result = ReceiveMessageResponse.builder();
    result.messages(msgs);
    return result.build();
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
    sqsConsumer.setQueue("queue");
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
    sqsConsumer.setQueue("SampleQueue");

    AmazonSQSConnection conn = new AmazonSQSConnection();
    //conn.setCredentials(StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey")));
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
