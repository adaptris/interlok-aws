/*
 * Copyright 2018 Adaptris
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.adaptris.aws.sns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.common.MetadataDataInputParameter;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.interlok.types.InterlokMessage;
import com.adaptris.util.GuidGenerator;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class TopicPublisherTest extends ExampleProducerCase {

  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    PublishToTopic producer =
        new PublishToTopic().withTopicArn("arn:aws:sns:us-east-1:123456789012:MyNewTopic");

    AmazonSNSConnection conn = new AmazonSNSConnection();
    AWSKeysAuthentication kauth = new AWSKeysAuthentication();
    kauth.setAccessKey("accessKey");
    kauth.setSecretKey("secretKey");
    conn.setCredentials(new StaticCredentialsBuilder().withAuthentication(kauth));
    conn.setRegion("My AWS Region");
    StandaloneProducer result = new StandaloneProducer(conn, producer);
    return result;
  }

  @Test
  public void testSubject() throws Exception {
    PublishToTopic producer =
        new PublishToTopic().withTopicArn("arn:aws:sns:us-east-1:123456789012:MyNewTopic");
    assertNull(producer.getSnsSubject());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMetadata("hello", "world");
    assertNull(producer.subject(msg));

    producer.withSubject("%message{hello}");
    assertEquals("world", producer.subject(msg));
  }

  @Test
  public void testSource() throws Exception {
    PublishToTopic producer =
        new PublishToTopic().withTopicArn("arn:aws:sns:us-east-1:123456789012:MyNewTopic");
    assertNull(producer.getSource());
    assertNotNull(producer.source());
    producer.withSource(new MetadataDataInputParameter());
    assertNotNull(producer.getSource());
    assertNotNull(producer.source());
    assertEquals(MetadataDataInputParameter.class, producer.source().getClass());
  }

  @Test
  public void testPublish() throws Exception {
    AmazonSNSClient mockClient = Mockito.mock(AmazonSNSClient.class);
    PublishResult mockResult = Mockito.mock(PublishResult.class);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.getMessageId()).thenReturn(resultId);
    Mockito.when(mockClient.publish(any())).thenReturn(mockResult);
    StandaloneProducer sp = initProducer(mockClient, "the subject", null);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(sp, msg);
    assertTrue(msg.headersContainsKey(PublishToTopic.SNS_MSG_ID_KEY));
    assertEquals(resultId, msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
  }

  @Test
  public void testPublish_NoSubject() throws Exception {
    AmazonSNSClient mockClient = Mockito.mock(AmazonSNSClient.class);
    PublishResult mockResult = Mockito.mock(PublishResult.class);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.getMessageId()).thenReturn(resultId);
    Mockito.when(mockClient.publish(any())).thenReturn(mockResult);

    StandaloneProducer sp = initProducer(mockClient, null, null);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(sp, msg);
    assertTrue(msg.headersContainsKey(PublishToTopic.SNS_MSG_ID_KEY));
    assertEquals(resultId, msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
  }

  @Test
  public void testPublish_Failure() throws Exception {
    AmazonSNSClient mockClient = Mockito.mock(AmazonSNSClient.class);
    PublishResult mockResult = Mockito.mock(PublishResult.class);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.getMessageId()).thenReturn(resultId);
    Mockito.when(mockClient.publish(any())).thenReturn(mockResult);
    PublishToTopic producer =
        new PublishToTopic().withTopicArn("arn:aws:sns:us-east-1:123456789012:MyNewTopic")
        .withSource(new DataInputParameter<String>() {
          @Override
          public String extract(InterlokMessage arg0) throws InterlokException {
            throw new InterlokException();
          }
        });

    StandaloneProducer sp =
        new StandaloneProducer(new MockAmazonSNSConnection(mockClient), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(sp, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  @Test
  public void testPublishWithSendAttributes() throws Exception {
    AmazonSNSClient mockClient = Mockito.mock(AmazonSNSClient.class);
    PublishResult mockResult = Mockito.mock(PublishResult.class);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.getMessageId()).thenReturn(resultId);
    when(mockClient.publish(any())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();

        PublishRequest request = (PublishRequest) args[0];
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

    StandaloneProducer sp = initProducer(mockClient, null, sendAttributes);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.replaceAllMetadata(List.of(new MetadataElement("myKey1", "myValue1"), new MetadataElement("myKey2", "myValue2"),
        new MetadataElement("myKey3", "myValue3")));
    ExampleServiceCase.execute(sp, msg);
    verify(mockClient).publish(any());
  }

  @Test
  public void testPublishWithSendAttributesOneMissing() throws Exception {
    AmazonSNSClient mockClient = Mockito.mock(AmazonSNSClient.class);
    PublishResult mockResult = Mockito.mock(PublishResult.class);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.getMessageId()).thenReturn(resultId);
    when(mockClient.publish(any())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();

        PublishRequest request = (PublishRequest) args[0];
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

    StandaloneProducer sp = initProducer(mockClient, null, sendAttributes);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.replaceAllMetadata(List.of(new MetadataElement("myKey1", "myValue1"), new MetadataElement("myKey3", "myValue3")));
    ExampleServiceCase.execute(sp, msg);
    verify(mockClient).publish(any());
  }

  @Test
  public void testPublishWithSendAttributesOneEmpty() throws Exception {
    AmazonSNSClient mockClient = Mockito.mock(AmazonSNSClient.class);
    PublishResult mockResult = Mockito.mock(PublishResult.class);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.getMessageId()).thenReturn(resultId);
    when(mockClient.publish(any())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();

        PublishRequest request = (PublishRequest) args[0];
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

    StandaloneProducer sp = initProducer(mockClient, null, sendAttributes);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.replaceAllMetadata(
        List.of(new MetadataElement("myKey1", "myValue1"), new MetadataElement("myKey2", ""), new MetadataElement("myKey3", "myValue3")));
    ExampleServiceCase.execute(sp, msg);
    verify(mockClient).publish(any());
  }

  private StandaloneProducer initProducer(AmazonSNSClient mockClient, String subject, List<String> sendAttributes) {
    PublishToTopic producer = new PublishToTopic().withTopicArn("arn:aws:sns:us-east-1:123456789012:MyNewTopic").withSubject(subject);

    if (sendAttributes != null) {
      producer.setSendAttributes(sendAttributes);
    }

    StandaloneProducer p = new StandaloneProducer(new MockAmazonSNSConnection(mockClient), producer);
    return p;
  }

  private class MockAmazonSNSConnection extends AmazonSNSConnection {
    private AmazonSNSClient mockClient;

    public MockAmazonSNSConnection(AmazonSNSClient client) {
      mockClient = client;
    }

    @Override
    protected void initConnection() throws CoreException {}

    @Override
    public AmazonSNSClient amazonClient() {
      return mockClient;
    }
  }

}
