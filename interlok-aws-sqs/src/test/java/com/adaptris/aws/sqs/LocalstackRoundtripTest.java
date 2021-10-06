package com.adaptris.aws.sqs;

import static com.adaptris.aws.sqs.LocalstackHelper.SQS_QUEUE;
import static com.adaptris.aws.sqs.LocalstackHelper.areTestsEnabled;
import static com.adaptris.aws.sqs.LocalstackHelper.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.FixedIntervalPoller;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.TimeInterval;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ListQueuesResult;

// A new local stack instance; send some messages, and then receive then.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackRoundtripTest {

  private static final String MSG_CONTENTS = "hello world";
  private LocalstackHelper helper;

  @Before
  public void setUp() throws Exception {
    helper = new LocalstackHelper();
  }

  @After
  public void tearDown() throws Exception {
    helper.shutdown();
  }

  @Test
  public void test_01_TestCreateQueue() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
    AmazonSQS sqs = helper.getSyncClient();
    sqs.createQueue(getProperty(SQS_QUEUE));
    ListQueuesResult result = sqs.listQueues();
    System.err.println(result.getQueueUrls());
  }

  @Test
  public void test_02_TestPublish() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
    AmazonSQSProducer sqsProducer = new AmazonSQSProducer().withQueue(getProperty(SQS_QUEUE));
    sqsProducer.withMessageAsyncCallback((e) -> {
      try {
        System.err.println(e.get().getMessageId());
      } catch (InterruptedException | ExecutionException e1) {
      }
    });
    AmazonSQSConnection conn = helper.createConnection();
    StandaloneProducer sp = new StandaloneProducer(conn, sqsProducer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);

    ExampleServiceCase.execute(sp, msg);

    assertTrue(helper.messagesOnQueue(helper.toQueueURL(getProperty(SQS_QUEUE))) > 0);
  }

  @Test
  public void test_03_TestConsume() throws Exception {
    StandaloneConsumer standaloneConsumer = null;
    try {
      Assume.assumeTrue(areTestsEnabled());
      AmazonSQSConsumer consumer = new AmazonSQSConsumer().withQueue(getProperty(SQS_QUEUE));
      AmazonSQSConnection conn = helper.createConnection();
      FixedIntervalPoller poller = new FixedIntervalPoller(new TimeInterval(300L, TimeUnit.MILLISECONDS));
      consumer.setPoller(poller);
      consumer.setPrefetchCount(1);

      standaloneConsumer = new StandaloneConsumer(conn, consumer);
      MockMessageListener listener = new MockMessageListener();
      standaloneConsumer.registerAdaptrisMessageListener(listener);
      // there is already a message from test_02
      LifecycleHelper.initAndStart(standaloneConsumer);

      BaseCase.waitForMessages(listener, 1, 5000);
      assertEquals(1, listener.getMessages().size());
      assertEquals(MSG_CONTENTS, listener.getMessages().get(0).getContent());
    } finally {
      LifecycleHelper.stopAndClose(standaloneConsumer);
    }
  }


  @Test
  public void test_99_TestDeleteQueue() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
    AmazonSQS sqs = helper.getSyncClient();
    sqs.deleteQueue(helper.toQueueURL(getProperty(SQS_QUEUE)));
  }

}
