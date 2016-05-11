package com.adaptris.core.aws.sqs;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AwsProducerTest extends ProducerCase {

  private static final String payload = "The quick brown fox jumps over the lazy dog.";
  
  private static final String queue = "queue";
  
  private AdaptrisMessageFactory messageFactory;
  private List<AdaptrisMessage> producedMessages;
  
  private AmazonSQSAsync sqsClientMock;
  private AmazonSQSConnection connectionMock;
  
  public AwsProducerTest(String params) {
    super(params);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    producedMessages = new ArrayList<AdaptrisMessage>();

    sqsClientMock = mock(AmazonSQSAsync.class);
    GetQueueUrlResult queueUrlResultMock = mock(GetQueueUrlResult.class);
    when(sqsClientMock.getQueueUrl((GetQueueUrlRequest)anyObject())).thenReturn(queueUrlResultMock);
    when(queueUrlResultMock.getQueueUrl()).thenReturn(queue);

    // Initialize SQS Connection mock
    connectionMock = mock(AmazonSQSConnection.class);
    when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(connectionMock);
    when(connectionMock.getASyncClient()).thenReturn(sqsClientMock);
    
    messageFactory = new DefaultMessageFactory();
  }
  
  public void testSingleProduce() throws Exception {
    AmazonSQSProducer producer = initialiseMockProducer();
    produce(1, producer);
    
    producer.stop();
    producer.close();
    verify(sqsClientMock).sendMessageAsync((SendMessageRequest)anyObject());
  }
  
  public void testSingleProduceWithException() throws Exception {
    when(sqsClientMock.sendMessageAsync((SendMessageRequest) anyObject())).thenThrow(new AmazonServiceException("Expected"));
    
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
  
  public void testMultipleProduce() throws Exception {
    final int numMsgs = 13;
    
    AmazonSQSProducer producer = initialiseMockProducer();
    produce(numMsgs, producer);

    verify(sqsClientMock, times(numMsgs)).sendMessageAsync((SendMessageRequest)anyObject());
  }
  
  public void testMultipleProduceWithDelay() throws Exception {
    final int numMsgs = 3;
    
    AmazonSQSProducer producer = initialiseMockProducer();
    producer.setDelaySeconds(1);
    produce(numMsgs, producer);

    verify(sqsClientMock, times(numMsgs)).sendMessageAsync((SendMessageRequest)anyObject());
  }

  public void testNoConnection() throws Exception {
    when(connectionMock.retrieveConnection(AmazonSQSConnection.class)).thenReturn(null);
    try {
      initialiseMockProducer();
      fail("Should not initialize without a connection.");
    } catch (CoreException ex) {
      // expected
    }
  }
  
  public void testNoDestination() throws Exception {
    AmazonSQSProducer producer = new AmazonSQSProducer();
    producer.registerConnection(connectionMock);
    
    try {
      LifecycleHelper.init(producer);
      fail("Should not initialize without a destination");
    } catch (CoreException ex) {
      // expected
    }
  }
  
  public void testSingleProduceWithSendAttributes() throws Exception {
    stub(sqsClientMock.sendMessageAsync((SendMessageRequest)anyObject())).toAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          
          SendMessageRequest request = (SendMessageRequest) args[0];
          assertTrue(request.getMessageAttributes() != null);
          assertEquals("myValue1", request.getMessageAttributes().get("myKey1").getStringValue());
          assertEquals("myValue2", request.getMessageAttributes().get("myKey2").getStringValue());
          assertEquals("myValue3", request.getMessageAttributes().get("myKey3").getStringValue());
          
          return null;
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
    verify(sqsClientMock).sendMessageAsync((SendMessageRequest)anyObject());
  }
  
  public void testSingleProduceWithSendAttributesOneMissing() throws Exception {
    stub(sqsClientMock.sendMessageAsync((SendMessageRequest)anyObject())).toAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          
          SendMessageRequest request = (SendMessageRequest) args[0];
          assertTrue(request.getMessageAttributes() != null);
          assertEquals("myValue1", request.getMessageAttributes().get("myKey1").getStringValue());
          assertNull(request.getMessageAttributes().get("myKey2"));
          assertEquals("myValue3", request.getMessageAttributes().get("myKey3").getStringValue());
          
          return null;
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
    verify(sqsClientMock).sendMessageAsync((SendMessageRequest)anyObject());
  }
  
  public void testSingleProduceWithSendAttributesOneEmpty() throws Exception {
    stub(sqsClientMock.sendMessageAsync((SendMessageRequest)anyObject())).toAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          
          SendMessageRequest request = (SendMessageRequest) args[0];
          assertTrue(request.getMessageAttributes() != null);
          assertEquals("myValue1", request.getMessageAttributes().get("myKey1").getStringValue());
          assertNull(request.getMessageAttributes().get("myKey2"));
          assertEquals("myValue3", request.getMessageAttributes().get("myKey3").getStringValue());
          
          return null;
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
    verify(sqsClientMock).sendMessageAsync((SendMessageRequest)anyObject());
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
    producer.setDestination(new ConfiguredProduceDestination("queue"));
    
    LifecycleHelper.init(producer);
    LifecycleHelper.start(producer);
    
    return producer;
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
    producer.setDestination(new ConfiguredProduceDestination("SampleQueue"));
    producer.setSendAttributes(sendAttributes);
    
    AmazonSQSConnection conn = new AmazonSQSConnection();
    conn.setAccessKey("My Access Key");
    conn.setSecretKey("My Security Key");
    conn.setRegion("My AWS Region");
    StandaloneProducer result = new StandaloneProducer(conn, producer);
    return result;
  }


}
