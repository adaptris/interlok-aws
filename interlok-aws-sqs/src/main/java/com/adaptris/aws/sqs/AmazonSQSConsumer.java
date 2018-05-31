package com.adaptris.aws.sqs;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.*;
import com.adaptris.core.fs.FsConsumerImpl;
import com.adaptris.core.fs.FsConsumerMonitor;
import com.adaptris.core.runtime.ParentRuntimeInfoComponent;
import com.adaptris.core.runtime.RuntimeInfoComponent;
import com.adaptris.core.runtime.RuntimeInfoComponentFactory;
import com.adaptris.core.runtime.WorkflowManager;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.amazonaws.services.sqs.AmazonSQS;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import javax.management.MalformedObjectNameException;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * <p>
 * Amazon Web Services SQS implementation of <code>AdaptrisMessageConsumer</code>.
 * </p>
 *
 * @config amazon-sqs-consumer
 * @license STANDARD
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-consumer")
@AdapterComponent
@ComponentProfile(summary = "Receive messages from Amazon SQS", tag = "consumer,amazon,sqs",
    recommended = {AmazonSQSConnection.class})
public class AmazonSQSConsumer extends AdaptrisPollingConsumer {

  private Integer prefetchCount;
  @AdvancedConfig
  private String ownerAwsAccountId;

  private transient Log log = LogFactory.getLog(this.getClass().getName());
  private transient AmazonSQS sqs;
  private transient String queueUrl;

  private transient List<String> receiveAttributes = Collections.singletonList("All");
  private transient List<String> receiveMessageAttributes = Collections.singletonList("All");

  static {
    RuntimeInfoComponentFactory.registerComponentFactory(new AmazonSQSConsumer.JmxFactory());
  }

  public AmazonSQSConsumer() {
    setReacquireLockBetweenMessages(true);
  }

  @Override
  public void start() throws CoreException {
    getSynClient();
    getQueueUrl();
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    // sqs = null;
  }

  /**
   * Read messages from the Amazon SQS queue and send on to Adapter processing.
   * <p>
   * Reads up to 10 messages at a time (the current maximum for Amazon SQS) then processes them one at a time,
   * deleting each after being successfully processed or moving on to the next if it fails processing.
   * </p>
   */
  @Override
  protected int processMessages() {
    int count = 0;

    try {
      List<Message> messages;

      messageCountLoop:
      do{
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        if(getPrefetchCount() != null) {
          receiveMessageRequest.setMaxNumberOfMessages(getPrefetchCount());
        }
        receiveMessageRequest.setAttributeNames(receiveAttributes);
        receiveMessageRequest.setMessageAttributeNames(receiveMessageAttributes);
        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        log.trace(messages.size() + " messages to process");

        for (Message message : messages) {
          try {
            AdaptrisMessage adpMsg = AdaptrisMessageFactory.defaultIfNull(getMessageFactory()).newMessage(message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
              adpMsg.addMetadata(entry.getKey(), entry.getValue());
            }
            for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
              adpMsg.addMetadata(entry.getKey(), entry.getValue().getStringValue());
            }

            retrieveAdaptrisMessageListener().onAdaptrisMessage(adpMsg);
            //
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));

            if (!continueProcessingMessages(++count)) {
                break messageCountLoop;
            }
          }
          catch (Exception e){
            log.error("Error processing message id: " + message.getMessageId(), e);
          }
        }
      } while (messages.size() > 0);
    }
    catch (Exception e){
      log.error("Error processing messages", e);
    }

    return count;
  }

  @Override
  protected void prepareConsumer() throws CoreException {
  }

  int messagesRemaining() throws CoreException {
    GetQueueAttributesResult result = getSynClient().getQueueAttributes(
        new GetQueueAttributesRequest(getQueueUrl()).withAttributeNames(QueueAttributeName.ApproximateNumberOfMessages));
    return Integer.valueOf(result.getAttributes().get(QueueAttributeName.ApproximateNumberOfMessages.toString()));
  }

  private AmazonSQS getSynClient() throws CoreException {
    if(sqs == null) {
      sqs = retrieveConnection(AmazonSQSConnection.class).getSyncClient();
    }
    return sqs;
  }

  private String getQueueUrl() throws CoreException {
    if(queueUrl == null) {
      GetQueueUrlRequest queueUrlRequest = new GetQueueUrlRequest(getDestination().getDestination());
      if (!StringUtils.isEmpty(getOwnerAwsAccountId())) {
        queueUrlRequest.withQueueOwnerAWSAccountId(getOwnerAwsAccountId());
      }
      queueUrl = getSynClient().getQueueUrl(queueUrlRequest).getQueueUrl();
    }
    return queueUrl;
  }

  public Integer getPrefetchCount() {
    return prefetchCount;
  }

  /**
   * The maximum number of messages to retrieve from the Amazon SQS queue per request. When omitted
   * the default setting on the queue will be used.
   * @param prefetchCount
   */
  public void setPrefetchCount(Integer prefetchCount) {
    this.prefetchCount = prefetchCount;
  }

  public String getOwnerAwsAccountId() {
    return ownerAwsAccountId;
  }

  /**
   * The AWS account ID of the account that created the queue. When omitted
   * the default setting on the queue will be used.
    * @param ownerAwsAccountId
   */
  public void setOwnerAwsAccountId(String ownerAwsAccountId) {
    this.ownerAwsAccountId = ownerAwsAccountId;
  }

  private static class JmxFactory extends RuntimeInfoComponentFactory {

    @Override
    protected boolean isSupported(AdaptrisComponent e) {
      if (e != null && e instanceof AmazonSQSConsumer) {
        return !isEmpty(((AmazonSQSConsumer) e).getUniqueId());
      }
      return false;
    }

    @Override
    protected RuntimeInfoComponent createComponent(ParentRuntimeInfoComponent parent, AdaptrisComponent e)
        throws MalformedObjectNameException {
      return new AmazonSQSConsumerMonitor((WorkflowManager) parent, (AmazonSQSConsumer) e);
    }

  }
}
