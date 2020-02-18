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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducer;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.cache.ExpiringMapCache;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
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
public class AmazonSQSProducer extends ProduceOnlyProducerImp {

  /**
   * Delay seconds for every message
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "0")
  private int delaySeconds = 0;
  
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
  
  private transient SendMessageAsyncCallback callback = (e) -> {  };
  
  private transient ExpiringMapCache cachedQueueURLs;
  
  public AmazonSQSProducer() {
    setSendAttributes(new ArrayList<String>());
    cachedQueueURLs = new ExpiringMapCache().withExpiration(new TimeInterval(1L, TimeUnit.HOURS)).withMaxEntries(1024);
  }

  public AmazonSQSProducer(ProduceDestination d) {
    this();
    setDestination(d);
  }

  
  @Override
  public void init() throws CoreException {
    try {
      LifecycleHelper.init(cachedQueueURLs);
      Args.notNull(retrieveConnection(AmazonSQSConnection.class), "connection");
      Args.notNull(getDestination(), "destination");
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
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
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    try {
      // Resolve the Queue URL from the destination and the message (in case of metadata destinations for example)
      String queueUrl = resolveQueueURL(msg);
      
      SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, msg.getContent());

      if (delaySeconds != 0) {
        sendMessageRequest.withDelaySeconds(delaySeconds);
      }
      
      applyMetadata(sendMessageRequest, msg);
      Future<SendMessageResult> future = getSQS().sendMessageAsync(sendMessageRequest);
      callback.handleResult(future);
    }
    catch (Exception e) {
      throw new ProduceException(e);
    }
  }

  private SendMessageRequest applyMetadata(SendMessageRequest sendMessageRequest, AdaptrisMessage msg) {
    if(this.getSendAttributes().size() > 0) {
      Map<String, MessageAttributeValue> attributes = new HashMap<>();
      for(String attribute : this.getSendAttributes()) {
        String adpMetadataValue = msg.getMetadataValue(attribute);
        if (!StringUtils.isEmpty(adpMetadataValue)) {
          MessageAttributeValue mav = new MessageAttributeValue();
          mav.setDataType("String");
          mav.setStringValue(msg.getMetadataValue(attribute));
          attributes.put(attribute, mav);          
        }
      }      
      sendMessageRequest.withMessageAttributes(attributes);
    }
    return sendMessageRequest;
  }
  
  private String resolveQueueURL(AdaptrisMessage msg) throws AmazonServiceException, AmazonClientException, CoreException {
    // Get destination (possibly from message)
    final String queueName = getDestination().getDestination(msg);
    String queueURL = (String) cachedQueueURLs.get(queueName);
    // It's not in the cache. Look up the queue url from Amazon and cache it.
    if(queueURL == null) {
      queueURL = AwsHelper.buildQueueUrl(queueName, getOwnerAwsAccountId(), getSQS());
      cachedQueueURLs.put(queueName, queueURL);
    }
    return queueURL;
  }

  @Override
  public void prepare() throws CoreException {
  }

  // Just for testing with localstack.
  protected AmazonSQSProducer withMessageAsyncCallback(SendMessageAsyncCallback callback) {
    this.callback = Args.notNull(callback, "callback");
    return this;
  }
  
  @FunctionalInterface
  public interface SendMessageAsyncCallback {
    void handleResult(Future<SendMessageResult> future);
  }
}
