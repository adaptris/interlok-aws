package com.adaptris.aws.sqs;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.CoreException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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

  private transient Log log = LogFactory.getLog(this.getClass().getName());
  private transient AmazonSQS sqs;
  private transient String queueUrl;

  private transient List<String> receiveAttributes = Collections.singletonList("All");
  private transient List<String> receiveMessageAttributes = Collections.singletonList("All");

  public AmazonSQSConsumer() {
    setReacquireLockBetweenMessages(true);
  }

  @Override
  public void start() throws CoreException {
    sqs = retrieveConnection(AmazonSQSConnection.class).getSyncClient();
    queueUrl = sqs.getQueueUrl(new GetQueueUrlRequest(getDestination().getDestination())).getQueueUrl();

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
}
