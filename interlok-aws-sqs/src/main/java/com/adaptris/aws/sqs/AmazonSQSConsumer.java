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

package com.adaptris.aws.sqs;

import static org.apache.commons.lang.StringUtils.isEmpty;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import javax.management.MalformedObjectNameException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisComponent;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.ConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.runtime.ParentRuntimeInfoComponent;
import com.adaptris.core.runtime.RuntimeInfoComponent;
import com.adaptris.core.runtime.RuntimeInfoComponentFactory;
import com.adaptris.core.runtime.WorkflowManager;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * <p>
 * Amazon Web Services SQS implementation of <code>AdaptrisMessageConsumer</code>.
 * </p>
 *
 * @config amazon-sqs-consumer
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-consumer")
@AdapterComponent
@ComponentProfile(summary = "Receive messages from Amazon SQS", tag = "consumer,amazon,sqs",
    recommended = {AmazonSQSConnection.class}, metadata= {"SQSMessageID"})
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

  public AmazonSQSConsumer(ConsumeDestination dest) {
    this();
    setDestination(dest);
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
            adpMsg.addMetadata("SQSMessageID", message.getMessageId());
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
    return Integer.parseInt(result.getAttributes().get(QueueAttributeName.ApproximateNumberOfMessages.toString()));
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
      if (!isEmpty(getOwnerAwsAccountId())) {
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
