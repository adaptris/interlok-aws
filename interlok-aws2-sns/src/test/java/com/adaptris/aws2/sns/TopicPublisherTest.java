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

package com.adaptris.aws2.sns;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.common.MetadataDataInputParameter;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.interlok.types.InterlokMessage;
import com.adaptris.util.GuidGenerator;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

public class TopicPublisherTest extends ExampleProducerCase {

  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    PublishToTopic producer = new PublishToTopic().withTopicArn("arn:aws2:sns:us-east-1:123456789012:MyNewTopic");
    AmazonSNSConnection conn = new AmazonSNSConnection();
    conn.setCredentials(StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey")));
    conn.setRegion("My AWS Region");
    StandaloneProducer result = new StandaloneProducer(conn, producer);
    return result;
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSubject() throws Exception {
    PublishToTopic producer = new PublishToTopic().withTopicArn("arn:aws2:sns:us-east-1:123456789012:MyNewTopic");
    assertNull(producer.getSnsSubject());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMetadata("hello", "world");
    assertNull(producer.resolveSubject(msg));
    producer.withSubject("%message{hello}");
    assertEquals("world", producer.resolveSubject(msg));
  }

  @Test
  public void testSource() throws Exception {
    PublishToTopic producer = new PublishToTopic().withTopicArn("arn:aws2:sns:us-east-1:123456789012:MyNewTopic");
    assertNull(producer.getSource());
    assertNotNull(producer.source());
    producer.withSource(new MetadataDataInputParameter());
    assertNotNull(producer.getSource());
    assertNotNull(producer.source());
    assertEquals(MetadataDataInputParameter.class, producer.source().getClass());
  }

  @Test
  public void testPublish() throws Exception {
    SnsClient mockClient = Mockito.mock(SnsClient.class);
    PublishResponse mockResult = Mockito.mock(PublishResponse.class);
    Mockito.when(mockClient.publish((PublishRequest)any())).thenReturn(mockResult);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.messageId()).thenReturn(resultId);
    PublishToTopic producer = new PublishToTopic()
        .withTopicArn("arn:aws2:sns:us-east-1:123456789012:MyNewTopic").withSubject("the subject");

    StandaloneProducer p =
        new StandaloneProducer(new MockAmazonSNSConnection(mockClient), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(p, msg);
    assertTrue(msg.headersContainsKey(PublishToTopic.SNS_MSG_ID_KEY));
    assertEquals(resultId, msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
  }

  @Test
  public void testPublish_NoSubject() throws Exception {
    SnsClient mockClient = Mockito.mock(SnsClient.class);
    PublishResponse mockResult = Mockito.mock(PublishResponse.class);
    Mockito.when(mockClient.publish((PublishRequest)any())).thenReturn(mockResult);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.messageId()).thenReturn(resultId);

    PublishToTopic producer =
        new PublishToTopic().withTopicArn("arn:aws2:sns:us-east-1:123456789012:MyNewTopic");
    StandaloneProducer p =
        new StandaloneProducer(new MockAmazonSNSConnection(mockClient), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(p, msg);
    assertTrue(msg.headersContainsKey(PublishToTopic.SNS_MSG_ID_KEY));
    assertEquals(resultId, msg.getMetadataValue(PublishToTopic.SNS_MSG_ID_KEY));
  }

  @Test
  public void testPublish_Failure() throws Exception {
    SnsClient mockClient = Mockito.mock(SnsClient.class);
    PublishResponse mockResult = Mockito.mock(PublishResponse.class);
    Mockito.when(mockClient.publish((PublishRequest)any())).thenReturn(mockResult);
    String resultId = new GuidGenerator().getUUID();
    Mockito.when(mockResult.messageId()).thenReturn(resultId);
    PublishToTopic producer =
        new PublishToTopic().withTopicArn("arn:aws2:sns:us-east-1:123456789012:MyNewTopic")
            .withSource(new DataInputParameter<String>() {
              @Override
              public String extract(InterlokMessage arg0) throws InterlokException {
                throw new InterlokException();
              }
            });

    StandaloneProducer p =
        new StandaloneProducer(new MockAmazonSNSConnection(mockClient), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(p, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  private class MockAmazonSNSConnection extends AmazonSNSConnection {
    private SnsClient mockClient;

    public MockAmazonSNSConnection(SnsClient client) {
      mockClient = client;
    }

    @Override
    protected void initConnection() throws CoreException {}

    @Override
    public SnsClient amazonClient() {
      return mockClient;
    }
  }

}
