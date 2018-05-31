package com.adaptris.aws.sqs;

import com.adaptris.core.runtime.ConsumerMonitorImpl;
import com.adaptris.core.runtime.WorkflowManager;

public class AmazonSQSConsumerMonitor extends ConsumerMonitorImpl<AmazonSQSConsumer> implements AmazonSQSConsumerMonitorMBean {

  public AmazonSQSConsumerMonitor(WorkflowManager owner, AmazonSQSConsumer consumer) {
    super(owner, consumer);
  }

  @Override
  public int messagesRemaining() {
    try {
      return getWrappedComponent().messagesRemaining();
    } catch (Exception e){
      return -1;
    }
  }
}
