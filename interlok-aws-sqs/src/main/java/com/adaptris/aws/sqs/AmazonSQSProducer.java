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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducer;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.cache.ExpiringMapCache;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.TimeInterval;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * {@link AdaptrisMessageProducer} implementation that sends messages to an Amazon Web Services (AWS) SQS queue.
 * <p>
 * Amazon SQS receives only text therefore only the message payload is sent as a string.
 * </p>
 *
 * @config amazon-sqs-producer
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-producer")
@AdapterComponent
@ComponentProfile(summary = "Send messages to Amazon SQS", tag = "producer,amazon,sqs",
recommended = {AmazonSQSConnection.class})
@DisplayOrder(order = {"queue", "ownerAwsAccountId", "delaySeconds", "sendAttributes"})
public class AmazonSQSProducer extends ProduceOnlyProducerImp {

  /**
   * Delay seconds for every message
   */
  @Getter
  @Setter
  @AdvancedConfig(rare = true)
  @InputFieldDefault(value = "0")
  private Integer delaySeconds;

  /**
   * Specify a list of a metadata keys that should be attached to a message.
   * <p>
   * Amazon SQS supports a limited set of attributes (10 at current count) that can be attached to a
   * message; use this list to specify the metadata keys that must be sent as attributes, otherwise
   * all metadata is ignored.
   * </p>
   *
   *
   */
  @XStreamImplicit(itemFieldName = "send-attribute")
  @NotNull
  @AutoPopulated
  @Getter
  @Setter
  @NonNull
  private List<String> sendAttributes;

  /**
   * The AWS account ID of the account that created the queue. When omitted the default setting on the
   * queue will be used.
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String ownerAwsAccountId;

  /**
   * The SQS Queue name
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NotBlank
  private String queue;

  /**
   * <p>
   * This parameter applies only to FIFO (first-in-first-out) queues.
   * </p>
   * <p>
   * The tag that specifies that a message belongs to a specific message group. Messages that belong to the same message group are processed
   * in a FIFO manner (however, messages in different message groups might be processed out of order). To interleave multiple ordered
   * streams within a single queue, use <code>MessageGroupId</code> values (for example, session data for multiple users).
   * </p>
   * <ul>
   * <li>
   * <p>
   * You must associate a non-empty <code>MessageGroupId</code> with a message. If you don't provide a <code>MessageGroupId</code>, the
   * action fails.
   * </p>
   * </li>
   * <li>
   * <p>
   * <code>ReceiveMessage</code> might return messages with multiple <code>MessageGroupId</code> values. For each
   * <code>MessageGroupId</code>, the messages are sorted by time sent. The caller can't specify a <code>MessageGroupId</code>.
   * </p>
   * </li>
   * </ul>
   * <p>
   * The length of <code>MessageGroupId</code> is 128 characters. Valid values: alphanumeric characters and punctuation
   * <code>(!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~)</code>.
   * </p>
   * <p>
   * For best practices of using <code>MessageGroupId</code>, see
   * <a href= "https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagegroupid-property.html" >Using the
   * MessageGroupId Property</a> in the <i>Amazon SQS Developer Guide</i>.
   * </p>
   * <important>
   * <p>
   * <code>MessageGroupId</code> is required for FIFO queues. You can't use it for Standard queues.
   * </p>
   * </important>
   *
   * @param messageGroupId
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @Size(max = 128)
  private String messageGroupId;


  private transient SendMessageAsyncCallback callback = (e) -> {  };

  private transient ExpiringMapCache cachedQueueURLs;


  public AmazonSQSProducer() {
    setSendAttributes(new ArrayList<String>());
    cachedQueueURLs = new ExpiringMapCache().withExpiration(new TimeInterval(1L, TimeUnit.HOURS)).withMaxEntries(1024);
  }

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getQueue(), "queue");
    LifecycleHelper.prepare(cachedQueueURLs);
  }


  @Override
  public void init() throws CoreException {
    LifecycleHelper.init(cachedQueueURLs);
    Args.notNull(retrieveConnection(AmazonSQSConnection.class), "connection");
  }

  private AmazonSQSAsync getSQS() throws CoreException {
    return retrieveConnection(AmazonSQSConnection.class).getASyncClient();
  }

  @Override
  public void start() throws CoreException {
    LifecycleHelper.start(cachedQueueURLs);
  }

  @Override
  public void stop() {
    LifecycleHelper.stop(cachedQueueURLs);
  }

  @Override
  public void close() {
    LifecycleHelper.close(cachedQueueURLs);
  }

  @Override
  public void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      String queueUrl = queueURL(endpoint);
      SendMessageRequest sendMessageRequest =
          configureDelay(new SendMessageRequest(queueUrl, msg.getContent())).withMessageGroupId(messageGroupId(msg));
      applyMetadata(sendMessageRequest, msg);
      Future<SendMessageResult> future = getSQS().sendMessageAsync(sendMessageRequest);
      callback.handleResult(future);
    }
    catch (Exception e) {
      throw new ProduceException(e);
    }
  }

  private SendMessageRequest configureDelay(SendMessageRequest req) {
    Optional.ofNullable(getDelaySeconds()).ifPresent((secs) -> req.setDelaySeconds(secs));
    return req;
  }

  private SendMessageRequest applyMetadata(SendMessageRequest request, AdaptrisMessage msg) {
    Map<String, MessageAttributeValue> attributes = null;
    if (CollectionUtils.isNotEmpty(getSendAttributes())) {
      attributes = new HashMap<>();
      for (String attribute : getSendAttributes()) {
        String adpMetadataValue = msg.getMetadataValue(attribute);
        if (StringUtils.isNotEmpty(adpMetadataValue)) {
          attributes.put(attribute, new MessageAttributeValue().withDataType("String").withStringValue(adpMetadataValue));
        }
      }
    }
    return request.withMessageAttributes(attributes);
  }

  private String queueURL(String queueName)
      throws AmazonServiceException, AmazonClientException, CoreException {
    String queueURL = (String) cachedQueueURLs.get(queueName);
    // It's not in the cache. Look up the queue url from Amazon and cache it.
    if(queueURL == null) {
      queueURL = AwsHelper.buildQueueUrl(queueName, getOwnerAwsAccountId(), getSQS());
      cachedQueueURLs.put(queueName, queueURL);
    }
    return queueURL;
  }

  protected String messageGroupId(AdaptrisMessage msg) throws Exception {
    return msg.resolve(getMessageGroupId());
  }

  // Just for testing with localstack.
  protected AmazonSQSProducer withMessageAsyncCallback(SendMessageAsyncCallback callback) {
    this.callback = Args.notNull(callback, "callback");
    return this;
  }


  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getQueue());
  }

  public AmazonSQSProducer withQueue(String s) {
    setQueue(s);
    return this;
  }


  @FunctionalInterface
  public interface SendMessageAsyncCallback {
    void handleResult(Future<SendMessageResult> future);
  }

}
