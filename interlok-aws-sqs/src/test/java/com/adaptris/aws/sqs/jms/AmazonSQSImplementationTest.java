package com.adaptris.aws.sqs.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.jms.JMSException;

import org.junit.Test;

import com.adaptris.aws.sqs.jms.AmazonSQSImplementation;

public class AmazonSQSImplementationTest {

  @Test
  public void testCreateFactory() throws Exception {
    AmazonSQSImplementation jmsImpl = new AmazonSQSImplementation();
    jmsImpl.setAccessKey("MyAccessKey");
    jmsImpl.setSecretKey("MyKey");
    jmsImpl.setRegion("eu-west-1");
    assertNotNull(jmsImpl.createConnectionFactory());
  }

  @Test
  public void testBadPassword() throws Exception {
    AmazonSQSImplementation jmsImpl = new AmazonSQSImplementation();
    jmsImpl.setAccessKey("MyAccessKey");
    jmsImpl.setSecretKey("PW:BACC");
    jmsImpl.setRegion("eu-west-1");
    try {
      jmsImpl.createConnectionFactory();
      fail();
    } catch (JMSException expected) {

    }
  }

  @Test
  public void testSetter() throws Exception {
    AmazonSQSImplementation jmsImpl = new AmazonSQSImplementation();
    assertNull(jmsImpl.getAccessKey());
    jmsImpl.setAccessKey("XXX");
    assertEquals("XXX", jmsImpl.getAccessKey());
    try {
      jmsImpl.setAccessKey(null);
      fail();
    } catch (IllegalArgumentException expected) {

    }

    assertNull(jmsImpl.getSecretKey());
    jmsImpl.setSecretKey("XXX");
    assertEquals("XXX", jmsImpl.getSecretKey());
    try {
      jmsImpl.setSecretKey(null);
      fail();
    } catch (IllegalArgumentException expected) {

    }

    assertNull(jmsImpl.getRegion());
    jmsImpl.setRegion("XXX");
    assertEquals("XXX", jmsImpl.getRegion());
    try {
      jmsImpl.setRegion(null);
      fail();
    } catch (IllegalArgumentException expected) {

    }

    assertNull(jmsImpl.getPrefetchCount());
    assertEquals(10, jmsImpl.prefetchCount());
    jmsImpl.setPrefetchCount(20);
    assertEquals(Integer.valueOf(20), jmsImpl.getPrefetchCount());
    assertEquals(20, jmsImpl.prefetchCount());
    jmsImpl.setPrefetchCount(null);
    assertEquals(10, jmsImpl.prefetchCount());

  }

}
