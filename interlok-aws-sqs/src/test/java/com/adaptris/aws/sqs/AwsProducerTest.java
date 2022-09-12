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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class AwsProducerTest extends ExampleProducerCase {

  private static final String payload = "The quick brown fox jumps over the lazy dog.";

  private static final String queue = "queue";

  private AdaptrisMessageFactory messageFactory;
  private List<AdaptrisMessage> producedMessages;

  private AmazonSQS sqsClientMock;
  private AmazonSQSConnection connectionMock;
  
  private SendMessageResult mockResult;
  private SdkHttpMetadata mockMetadata;

  @Before
  public void setUp() throws Exception {

    producedMessages = new ArrayList<AdaptrisMessage>();

    sqsClientMock = mock(AmazonSQS.class);
    GetQueueUrlResult queueUrlResultMock = mock(GetQueueUrlResult.class);
    when(sqsClientMock.getQueueUrl((GetQueueUrlRequest) any())).thenReturn(queueUrlResultMock);
    when(queueUrlResultMock.getQueueUrl()).thenReturn(queue);

    // Initialize SQS Connection mock
    connectionMock = mock(AmazonSQSConnection.class);
    when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.getSyncClient()).thenReturn(sqsClientMock);

    messageFactory = new DefaultMessageFactory();
    
    mockResult = mock(SendMessageResult.class);
    mockMetadata = mock(SdkHttpMetadata.class);
    when(mockResult.getSdkHttpMetadata()).thenReturn(mockMetadata);
    when(mockMetadata.getHttpStatusCode()).thenReturn(200);
    when(sqsClientMock.sendMessage((SendMessageRequest) any())).thenReturn(mockResult);
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
        .thenThrow(new AmazonServiceException("Expected"));

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

  @Test(expected = IllegalArgumentException.class)
  public void testNoConnection() throws Exception {
    when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(null);
    initialiseMockProducer();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoDestination() throws Exception {
    AmazonSQSProducer producer = new AmazonSQSProducer();
    LifecycleHelper.prepare(producer);
    LifecycleHelper.init(producer);
  }

  @Test
  public void testSingleProduceWithSendAttributes() throws Exception {
    when(sqsClientMock.sendMessage((SendMessageRequest) any()))
        .thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();

          SendMessageRequest request = (SendMessageRequest) args[0];
          assertTrue(request.getMessageAttributes() != null);
          assertEquals("myValue1", request.getMessageAttributes().get("myKey1").getStringValue());
          assertEquals("myValue2", request.getMessageAttributes().get("myKey2").getStringValue());
          assertEquals("myValue3", request.getMessageAttributes().get("myKey3").getStringValue());

          return mockResult;
      }
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
        .thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();

          SendMessageRequest request = (SendMessageRequest) args[0];
          assertTrue(request.getMessageAttributes() != null);
          assertEquals("myValue1", request.getMessageAttributes().get("myKey1").getStringValue());
          assertNull(request.getMessageAttributes().get("myKey2"));
          assertEquals("myValue3", request.getMessageAttributes().get("myKey3").getStringValue());

          return mockResult;
      }
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
        .thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();

          SendMessageRequest request = (SendMessageRequest) args[0];
          assertTrue(request.getMessageAttributes() != null);
          assertEquals("myValue1", request.getMessageAttributes().get("myKey1").getStringValue());
          assertNull(request.getMessageAttributes().get("myKey2"));
          assertEquals("myValue3", request.getMessageAttributes().get("myKey3").getStringValue());

          return mockResult;
      }
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
    conn.setCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
    conn.setRegion("My AWS Region");
    StandaloneProducer result = new StandaloneProducer(conn, producer);
    return result;
  }


}
