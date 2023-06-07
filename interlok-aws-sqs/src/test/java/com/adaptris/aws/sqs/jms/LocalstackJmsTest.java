package com.adaptris.aws.sqs.jms;

import static com.adaptris.aws.sqs.LocalstackHelper.SQS_JMS_QUEUE;
import static com.adaptris.aws.sqs.LocalstackHelper.areTestsEnabled;
import static com.adaptris.aws.sqs.LocalstackHelper.getProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import com.adaptris.aws.sqs.LocalstackHelper;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.jms.JmsConnection;
import com.adaptris.core.jms.PtpConsumer;
import com.adaptris.core.jms.PtpProducer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ListQueuesResult;

// A new local stack instance; send some messages, and then receive then.
@TestMethodOrder(MethodOrderer.MethodName.class)
public class LocalstackJmsTest {

  private static final String MSG_CONTENTS = "hello world";
  private LocalstackHelper helper;

  @BeforeEach
  public void setUp() throws Exception {
    helper = new LocalstackHelper();
  }

  @AfterEach
  public void tearDown() throws Exception {
    helper.shutdown();
  }

  @Test
  public void test_01_TestCreateQueue() throws Exception {
    assumeTrue(areTestsEnabled());
    AmazonSQS sqs = helper.getSyncClient();
    sqs.createQueue(getProperty(SQS_JMS_QUEUE));
    ListQueuesResult result = sqs.listQueues();
    System.err.println(result.getQueueUrls());
  }

  @Test
  public void test_02_TestPublish() throws Exception {
    assumeTrue(areTestsEnabled());
    PtpProducer producer = new PtpProducer().withQueue(getProperty(SQS_JMS_QUEUE));
    JmsConnection conn = helper.createJmsConnection();
    StandaloneProducer sp = new StandaloneProducer(conn, producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);
    ExampleServiceCase.execute(sp, msg);
    assertTrue(helper.messagesOnQueue(helper.toQueueURL(getProperty(SQS_JMS_QUEUE))) > 0);
  }

  @Test
  public void test_03_TestConsume() throws Exception {
    StandaloneConsumer standaloneConsumer = null;
    long start = System.currentTimeMillis();
    try {
      assumeTrue(areTestsEnabled());
      PtpConsumer consumer = new PtpConsumer().withQueue(getProperty(SQS_JMS_QUEUE));
      JmsConnection conn = helper.createJmsConnection();
      // System.err.println("Create Objects : " + (System.currentTimeMillis() - start));
      standaloneConsumer = new StandaloneConsumer(conn, consumer);
      MockMessageListener listener = new MockMessageListener();
      standaloneConsumer.registerAdaptrisMessageListener(listener);
      // there is already a message from test_02
      // System.err.println("Create Consumer : " + (System.currentTimeMillis() - start));
      LifecycleHelper.initAndStart(standaloneConsumer);
      // System.err.println("Start Consumer : " + (System.currentTimeMillis() - start));
      BaseCase.waitForMessages(listener, 1, 20000);
      // System.err.println("Finish Consume : " + (System.currentTimeMillis() - start));
      assertEquals(1, listener.getMessages().size());
      assertEquals(MSG_CONTENTS, listener.getMessages().get(0).getContent());
    } finally {
      final StandaloneConsumer sc = standaloneConsumer;
      // Stop + Close seems to take 20 seconds or so, so let's spawn it off
      ManagedThreadFactory.createThread(() -> {
        LifecycleHelper.stopAndClose(sc);
      }).start();
    }
  }


  @Test
  public void test_99_TestDeleteQueue() throws Exception {
    assumeTrue(areTestsEnabled());
    AmazonSQS sqs = helper.getSyncClient();
    sqs.deleteQueue(helper.toQueueURL(getProperty(SQS_JMS_QUEUE)));
  }

}
