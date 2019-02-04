package com.adaptris.aws.sqs;

import static com.adaptris.aws.sqs.LocalstackHelper.SQS_QUEUE;
import static com.adaptris.aws.sqs.LocalstackHelper.areTestsEnabled;
import static com.adaptris.aws.sqs.LocalstackHelper.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.BaseCase;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.FixedIntervalPoller;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;

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
    if (areTestsEnabled()) {
      AmazonSQS sqs = helper.getSyncClient();
      sqs.createQueue(getProperty(SQS_QUEUE));
      ListQueuesResult result = sqs.listQueues();
      System.err.println(result.getQueueUrls());
    } else {
      System.err.println("localstack disabled; not executing test_01_TestCreateQueue");
    }
  }
  
  @Test
  public void test_02_TestPublish() throws Exception {
    if (areTestsEnabled()) {
      AmazonSQSProducer sqsProducer = new AmazonSQSProducer(new ConfiguredProduceDestination(getProperty(SQS_QUEUE)));
      AmazonSQSConnection conn = helper.createConnection();
      StandaloneProducer sp = new StandaloneProducer(conn, sqsProducer);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS);

      ServiceCase.execute(sp, msg);
      
      // Since we send async we have to wait for messages to appear :(
      AmazonSQS sqs = helper.getSyncClient();
      helper.waitForMessagesToAppear(getProperty(SQS_QUEUE), 1, 30000);
      assertTrue(helper.messagesOnQueue(helper.toQueueURL(getProperty(SQS_QUEUE))) > 0);
    }
    else {
      System.err.println("localstack disabled; not executing test_02_TestPublish");
    }
  }

//  @Test
//  public void test_03_TestConsume() throws Exception {
//    StandaloneConsumer standaloneConsumer = null;
//    try {
//      if (areTestsEnabled()) {
//        AmazonSQSConsumer consumer = new AmazonSQSConsumer(new ConfiguredConsumeDestination(getProperty(SQS_QUEUE)));
//        AmazonSQSConnection conn = helper.createConnection();
//        FixedIntervalPoller poller = new FixedIntervalPoller(new TimeInterval(300L, TimeUnit.MILLISECONDS));
//        consumer.setPoller(poller);
//
//        standaloneConsumer = new StandaloneConsumer(conn, consumer);
//        MockMessageListener listener = new MockMessageListener();
//        standaloneConsumer.registerAdaptrisMessageListener(listener);
//        // there is already a message from test_02
//        LifecycleHelper.initAndStart(standaloneConsumer);
//        
//        BaseCase.waitForMessages(listener,1, 20000);
//        assertEquals(1, listener.getMessages().size());
//      }
//      else {
//        System.err.println("localstack disabled; not executing test_02_TestPublish");
//      }
//    }
//    finally {
//      LifecycleHelper.stopAndClose(standaloneConsumer);
//    }
//  }
  
  
  @Test
  public void test_99_TestDeleteQueue() throws Exception {
    if (areTestsEnabled()) {
      AmazonSQS sqs = helper.getSyncClient();
      sqs.deleteQueue(helper.toQueueURL(getProperty(SQS_QUEUE)));
    } else {
      System.err.println("localstack disabled; not executing test_99_TestDeleteQueue");
    }
  }

}
