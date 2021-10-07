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

package com.adaptris.aws2.sqs;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisComponent;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.CoreException;
import com.adaptris.core.runtime.ParentRuntimeInfoComponent;
import com.adaptris.core.runtime.RuntimeInfoComponent;
import com.adaptris.core.runtime.RuntimeInfoComponentFactory;
import com.adaptris.core.runtime.WorkflowManager;
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import javax.management.MalformedObjectNameException;
import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * <p>
 * Amazon Web Services SQS implementation of <code>AdaptrisMessageConsumer</code>.
 * </p>
 *
 * @config amazon-sqs-consumer
 * @since 3.0.3
 */
@XStreamAlias("aws2-amazon-sqs-consumer")
@AdapterComponent
@ComponentProfile(summary = "Receive messages from Amazon SQS", tag = "consumer,amazon,sqs",
    recommended = {AmazonSQSConnection.class}, metadata= {"SQSMessageID"})
@DisplayOrder(order = {"queue", "ownerAwsAccountId", "alwaysDelete", "prefetchCount"})
public class AmazonSQSConsumer extends AdaptrisPollingConsumer {

  /**
   * The maximum number of messages to retrieve from the Amazon SQS queue per request. When omitted
   * the default setting on the queue will be used.
   *
   */
  @Getter
  @Setter
  private Integer prefetchCount;
  /**
   * The AWS account ID of the account that created the queue. When omitted the default setting on the
   * queue will be used.
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String ownerAwsAccountId;

  /**
   * Whether or not to always delete the message from the queue.
   *
   * <p>
   * Since the workflow can be configured weith complex behaviour on errors, we have traditionally deleted the message once
   * submitted to the workflow. In the specific AWS instance, this means that any configured 'Dead Letter Queue' behaviour will
   * since AWS will consider the message to have been successfully delivered.
   * </p>
   * If set to false, then messages will be deleted once they are successfully processed. The default value is 'true' to preserve
   * backwards compatible behaviour
   * </p>
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "true")
  private Boolean alwaysDelete;

  /**
   * The SQS Queue name
   *
   */
  @Getter
  @Setter
  @NotBlank
  private String queue;


  private transient String queueUrl = null;
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
    try
    {
      getQueueUrl();
    }
    catch (Exception e)
    {
      ExceptionHelper.rethrowCoreException(e);
    }
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    queueUrl = null;
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
      final String myQueueUrl = getQueueUrl();

      messageCountLoop:
      do{
        ReceiveMessageRequest.Builder builder = ReceiveMessageRequest.builder();
        builder.queueUrl(myQueueUrl);
        if(getPrefetchCount() != null) {
          builder.maxNumberOfMessages(getPrefetchCount());
        }

        List<QueueAttributeName> names = new ArrayList<>();
        for (String ra : receiveAttributes)
        {
          names.add(QueueAttributeName.fromValue(ra));
        }
        builder.attributeNames(names);


        builder.messageAttributeNames(receiveMessageAttributes);
        messages = getSynClient().receiveMessage(builder.build()).messages();
        log.trace(messages.size() + " messages to process");

        for (Message message : messages) {
          try {
            AdaptrisMessage adpMsg = AdaptrisMessageFactory.defaultIfNull(getMessageFactory()).newMessage(message.body());
            final String handle = message.receiptHandle();
            adpMsg.addMetadata("SQSMessageID", message.messageId());
            for (Entry<String, String> entry : message.attributesAsStrings().entrySet()) {
              adpMsg.addMetadata(entry.getKey(), entry.getValue());
            }
            for (Entry<String, MessageAttributeValue> entry : message.messageAttributes().entrySet()) {
              adpMsg.addMetadata(entry.getKey(), entry.getValue().stringValue());
            }
            if (alwaysDelete()) {
              retrieveAdaptrisMessageListener().onAdaptrisMessage(adpMsg);
              getSynClient().deleteMessage(DeleteMessageRequest.builder().queueUrl(myQueueUrl).receiptHandle(handle).build());
            } else {
              retrieveAdaptrisMessageListener().onAdaptrisMessage(adpMsg, (m) -> {
                getSynClient().deleteMessage(DeleteMessageRequest.builder().queueUrl(myQueueUrl).receiptHandle(handle).build());
              });
            }
            if (!continueProcessingMessages(++count)) {
                break messageCountLoop;
            }
          }
          catch (Exception e){
            log.error("Error processing message id: " + message.messageId(), e);
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
    Args.notBlank(getQueue(), "queue");
  }

  public AmazonSQSConsumer withQueue(String s) {
    setQueue(s);
    return this;
  }

  int messagesRemaining() throws CoreException, ExecutionException, InterruptedException
  {
    GetQueueAttributesRequest.Builder builder = GetQueueAttributesRequest.builder();
    builder.queueUrl(getQueueUrl());
    builder.attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
    GetQueueAttributesResponse result = getSynClient().getQueueAttributes(builder.build());
    Map<QueueAttributeName, String> attributes = result.attributes();
    return Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
  }

  private String queueName() {
    return getQueue();
  }

  @SneakyThrows(CoreException.class)
  private SqsClient getSynClient() {
    return retrieveConnection(AmazonSQSConnection.class).getSyncClient();
  }

  private String getQueueUrl() throws CoreException, ExecutionException, InterruptedException {
    if (queueUrl == null) {
      queueUrl = AwsHelper.buildQueueUrl(queueName(), getOwnerAwsAccountId(), getSynClient());
    }
    return queueUrl;
  }

  private boolean alwaysDelete() {
    return BooleanUtils.toBooleanDefaultIfNull(getAlwaysDelete(), true);
  }

  @Override
  protected String newThreadName() {
    return DestinationHelper.threadName(retrieveAdaptrisMessageListener());
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
