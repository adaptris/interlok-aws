package com.adaptris.aws.sqs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.runtime.RuntimeInfoComponentFactory;
import com.adaptris.core.runtime.WorkflowManager;
import com.adaptris.util.GuidGenerator;

public class ConsumerMonitorTest {

  @Test
  public void testMessagesRemaining() throws Exception {
    AmazonSQSConsumer consumer = Mockito.mock(AmazonSQSConsumer.class);
    WorkflowManager workflowManager = Mockito.mock(WorkflowManager.class);
    Mockito.when(consumer.messagesRemaining()).thenThrow(new RuntimeException()).thenReturn(0);
    AmazonSQSConsumerMonitor monitor = new AmazonSQSConsumerMonitor(workflowManager, consumer);
    assertEquals(-1, monitor.messagesRemaining());
    assertEquals(0, monitor.messagesRemaining());
  }

  @Test
  public void testJmxFactory() throws Exception {
    WorkflowManager workflowManager = Mockito.mock(WorkflowManager.class);
    AmazonSQSConsumer consumer = new AmazonSQSConsumer().withQueue("myQueue");
    AmazonSQSConsumer consumerWithId = new AmazonSQSConsumer().withQueue("myQueue");
    consumerWithId.setUniqueId(new GuidGenerator().getUUID());
    AmazonSQSProducer producer = new AmazonSQSProducer().withQueue("myQueue");
    assertNull(RuntimeInfoComponentFactory.create(workflowManager, null));
    assertNull(RuntimeInfoComponentFactory.create(workflowManager, producer));
    assertNull(RuntimeInfoComponentFactory.create(workflowManager, consumer));
    assertNotNull(RuntimeInfoComponentFactory.create(workflowManager, consumerWithId));
  }
}
