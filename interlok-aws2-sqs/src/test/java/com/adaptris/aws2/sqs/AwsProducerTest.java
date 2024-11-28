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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class AwsProducerTest extends ExampleProducerCase {

  private static final String payload = "The quick brown fox jumps over the lazy dog.";

  private static final String queue = "queue";

  private AdaptrisMessageFactory messageFactory;
  private List<AdaptrisMessage> producedMessages;

  private SqsAsyncClient sqsClientMock;
  private AmazonSQSConnection connectionMock;

  @BeforeEach
  public void setUp() throws Exception {

    producedMessages = new ArrayList<>();

    sqsClientMock = mock(SqsAsyncClient.class);
    GetQueueUrlResponse queueUrlResultMock = mock(GetQueueUrlResponse.class);
    when(sqsClientMock.getQueueUrl((GetQueueUrlRequest) any())).thenReturn(CompletableFuture.completedFuture(queueUrlResultMock));
    when(queueUrlResultMock.queueUrl()).thenReturn(queue);

    // Initialize SQS Connection mock
    connectionMock = mock(AmazonSQSConnection.class);
    when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.getASyncClient()).thenReturn(sqsClientMock);

    messageFactory = new DefaultMessageFactory();
  }

  @Test
  public void testSingleProduce() throws Exception {
    AmazonSQSProducer producer = initialiseMockProducer();
    produce(1, producer);

    producer.stop();
    producer.close();
    verify(sqsClientMock).sendMessage((SendMessageRequest) any());
  }

  @Test
  public void testSingleProduceWithException() throws Exception {
    when(sqsClientMock.sendMessage((SendMessageRequest) any()))
        .thenThrow(AwsServiceException.create("Expected", new Exception()));

    AmazonSQSProducer producer = initialiseMockProducer();
    try {
      produce(1, producer);
      fail("Should fail via an AmazonServiceException");
    } catch (CoreException ex) {
      //expected
    }

    producer.stop();
    producer.close();
  }

  @Test
  public void testMultipleProduce() throws Exception {
    final int numMsgs = 13;

    AmazonSQSProducer producer = initialiseMockProducer();
    produce(numMsgs, producer);

    verify(sqsClientMock, times(numMsgs)).sendMessage((SendMessageRequest) any());
  }

  @Test
  public void testMultipleProduceWithDelay() throws Exception {
    final int numMsgs = 3;

    AmazonSQSProducer producer = initialiseMockProducer();
    producer.setDelaySeconds(1);
    produce(numMsgs, producer);

    verify(sqsClientMock, times(numMsgs)).sendMessage((SendMessageRequest) any());
  }

  @Test
  public void testNoConnection() throws Exception {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(null);
      initialiseMockProducer();
    });
  }

  @Test
  public void testNoDestination() throws Exception {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      AmazonSQSProducer producer = new AmazonSQSProducer();
      LifecycleHelper.prepare(producer);
      LifecycleHelper.init(producer);
    });
  }

  @Test
  public void testSingleProduceWithSendAttributes() throws Exception {
    when(sqsClientMock.sendMessage((SendMessageRequest) any()))
        .thenAnswer(invocation ->
        {
            Object[] args = invocation.getArguments();

            SendMessageRequest request = (SendMessageRequest) args[0];
            assertTrue(request.messageAttributes() != null);
            assertEquals("myValue1", request.messageAttributes().get("myKey1").stringValue());
            assertEquals("myValue2", request.messageAttributes().get("myKey2").stringValue());
            assertEquals("myValue3", request.messageAttributes().get("myKey3").stringValue());

            return null;
        });

    List<String> sendAttributes = new ArrayList<>();
    sendAttributes.add("myKey1");
    sendAttributes.add("myKey2");
    sendAttributes.add("myKey3");

    AmazonSQSProducer producer = initialiseMockProducer(sendAttributes);

    AdaptrisMessage msg = this.createMessage(new MetadataElement("myKey1", "myValue1"), new MetadataElement("myKey2", "myValue2"), new MetadataElement("myKey3", "myValue3"));
    producer.produce(msg);

    producer.stop();
    producer.close();
    verify(sqsClientMock).sendMessage((SendMessageRequest) any());
  }

  @Test
  public void testSingleProduceWithSendAttributesOneMissing() throws Exception {
    when(sqsClientMock.sendMessage((SendMessageRequest) any()))
        .thenAnswer(invocation ->
        {
            Object[] args = invocation.getArguments();

            SendMessageRequest request = (SendMessageRequest) args[0];
            assertTrue(request.messageAttributes() != null);
            assertEquals("myValue1", request.messageAttributes().get("myKey1").stringValue());
            assertNull(request.messageAttributes().get("myKey2"));
            assertEquals("myValue3", request.messageAttributes().get("myKey3").stringValue());

            return null;
        });

    List<String> sendAttributes = new ArrayList<>();
    sendAttributes.add("myKey1");
    sendAttributes.add("myKey2");
    sendAttributes.add("myKey3");

    AmazonSQSProducer producer = initialiseMockProducer(sendAttributes);

    AdaptrisMessage msg = this.createMessage(new MetadataElement("myKey1", "myValue1"), new MetadataElement("myKey3", "myValue3"));
    producer.produce(msg);

    producer.stop();
    producer.close();
    verify(sqsClientMock).sendMessage((SendMessageRequest) any());
  }

  @Test
  public void testSingleProduceWithSendAttributesOneEmpty() throws Exception {
    when(sqsClientMock.sendMessage((SendMessageRequest) any()))
        .thenAnswer(invocation ->
        {
            Object[] args = invocation.getArguments();

            SendMessageRequest request = (SendMessageRequest) args[0];
            assertTrue(request.messageAttributes() != null);
            assertEquals("myValue1", request.messageAttributes().get("myKey1").stringValue());
            assertNull(request.messageAttributes().get("myKey2"));
            assertEquals("myValue3", request.messageAttributes().get("myKey3").stringValue());

            return null;
        });

    List<String> sendAttributes = new ArrayList<>();
    sendAttributes.add("myKey1");
    sendAttributes.add("myKey2");
    sendAttributes.add("myKey3");

    AmazonSQSProducer producer = initialiseMockProducer(sendAttributes);

    AdaptrisMessage msg = this.createMessage(new MetadataElement("myKey1", "myValue1"), new MetadataElement("myKey2", ""), new MetadataElement("myKey3", "myValue3"));
    producer.produce(msg);

    producer.stop();
    producer.close();
    verify(sqsClientMock).sendMessage((SendMessageRequest) any());
  }

  private void produce(final int numMsgs, AmazonSQSProducer producer) throws Exception{
    for (int i=0; i < numMsgs; i++){
      producer.produce(createMessage());
    }
  }

  private AdaptrisMessage createMessage(){
    AdaptrisMessage msg = messageFactory.newMessage(payload);
    producedMessages.add(msg);
    return msg;
  }

  private AdaptrisMessage createMessage(MetadataElement ... metadataElements ){
    AdaptrisMessage msg = this.createMessage();
    for(MetadataElement element : metadataElements)
      msg.addMetadata(element);
    return msg;
  }

  private AmazonSQSProducer initialiseMockProducer() throws Exception {
    AmazonSQSProducer producer = new AmazonSQSProducer();
    producer.registerConnection(connectionMock);
    producer.setQueue("queue");

    return LifecycleHelper.initAndStart(producer);
  }

  private AmazonSQSProducer initialiseMockProducer(List<String> sendAttributes) throws Exception {
    AmazonSQSProducer mockProducer = this.initialiseMockProducer();
    mockProducer.setSendAttributes(sendAttributes);

    return mockProducer;
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    List<String> sendAttributes = new ArrayList<>();
    sendAttributes.add("myMetadataKey1");
    sendAttributes.add("myMetadataKey2");
    sendAttributes.add("myMetadataKey3");

    AmazonSQSProducer producer = new AmazonSQSProducer();
    producer.setQueue("SampleQueue");
    producer.setSendAttributes(sendAttributes);

    AmazonSQSConnection conn = new AmazonSQSConnection();
    //conn.setCredentials(StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey")));
    conn.setRegion("My AWS Region");
    StandaloneProducer result = new StandaloneProducer(conn, producer);
    return result;
  }


}
