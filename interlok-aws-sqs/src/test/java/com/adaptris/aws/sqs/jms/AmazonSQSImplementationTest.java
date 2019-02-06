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

package com.adaptris.aws.sqs.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import javax.jms.JMSException;
import org.junit.Test;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.aws.sqs.BufferedSQSClientFactory;
import com.adaptris.aws.sqs.UnbufferedSQSClientFactory;

public class AmazonSQSImplementationTest {

  @Test
  public void testCreateFactory() throws Exception {
    AmazonSQSImplementation jmsImpl = createImpl();
    jmsImpl.setAuthentication(new AWSKeysAuthentication("MyAccessKey", "MyKey"));
    jmsImpl.setRegion("eu-west-1");
    assertNotNull(jmsImpl.createConnectionFactory());
  }

  @Test
  public void testBadPassword() throws Exception {
    AmazonSQSImplementation jmsImpl = createImpl();
    jmsImpl.setAuthentication(new AWSKeysAuthentication("MyAccessKey", "PW:BACCy"));
    jmsImpl.setRegion("eu-west-1");
    try {
      jmsImpl.createConnectionFactory();
      fail();
    } catch (JMSException expected) {

    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSetter() throws Exception {
    AmazonSQSImplementation jmsImpl = createImpl();
    assertNull(jmsImpl.getAccessKey());
    jmsImpl.setAccessKey("XXX");
    assertEquals("XXX", jmsImpl.getAccessKey());
    assertNull(jmsImpl.getSecretKey());
    jmsImpl.setSecretKey("XXX");
    assertEquals("XXX", jmsImpl.getSecretKey());
    assertNull(jmsImpl.getRegion());
    jmsImpl.setRegion("XXX");
    assertEquals("XXX", jmsImpl.getRegion());
    assertNull(jmsImpl.getPrefetchCount());
    assertEquals(10, jmsImpl.prefetchCount());
    jmsImpl.setPrefetchCount(20);
    assertEquals(Integer.valueOf(20), jmsImpl.getPrefetchCount());
    assertEquals(20, jmsImpl.prefetchCount());
    jmsImpl.setPrefetchCount(null);
    assertEquals(10, jmsImpl.prefetchCount());

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testAuthentication() throws Exception {
    AmazonSQSImplementation jmsImpl = createImpl();
    assertNull(jmsImpl.getAuthentication());
    assertNotNull(jmsImpl.authentication());
    assertEquals(DefaultAWSAuthentication.class, jmsImpl.authentication().getClass());
    
    jmsImpl.setAccessKey("mykey");
    jmsImpl.setSecretKey("mySecret");
    assertNotNull(jmsImpl.authentication());
    assertNull(jmsImpl.getAuthentication());
    assertEquals(AWSKeysAuthentication.class, jmsImpl.authentication().getClass());

    jmsImpl.withAuthentication(new DefaultAWSAuthentication());
    assertNotNull(jmsImpl.getAuthentication());
    assertEquals(DefaultAWSAuthentication.class, jmsImpl.authentication().getClass());
  }
  
  @Test
  public void testClientFactory() throws Exception {
    AmazonSQSImplementation jmsImpl = createImpl();
    assertNotNull(jmsImpl.getSqsClientFactory());
    assertEquals(UnbufferedSQSClientFactory.class, jmsImpl.getSqsClientFactory().getClass());

    jmsImpl.withClientFactory(new BufferedSQSClientFactory());
    assertEquals(BufferedSQSClientFactory.class, jmsImpl.getSqsClientFactory().getClass());   
  }
  

  @Test
  public void testConnectionEquals() throws Exception {
    AmazonSQSImplementation jmsImpl = createImpl();
    assertFalse(jmsImpl.connectionEquals(new AmazonSQSImplementation()));
    assertTrue(jmsImpl.connectionEquals(jmsImpl));
  }

  protected AmazonSQSImplementation createImpl() {
    return new AmazonSQSImplementation();
  }
}
