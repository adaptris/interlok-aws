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
import java.util.concurrent.ConcurrentHashMap;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducer;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import org.apache.commons.lang.StringUtils;

/**
 * {@link AdaptrisMessageProducer} implementation that sends messages to an Amazon Web Services (AWS) SQS queue.
 * <p>
 * Amazon SQS receives only text therefore only the message payload is sent as a string.
 * </p>
 * 
 * @config amazon-sqs-producer
 * @license STANDARD
 * @since 3.0.3
 */
@XStreamAlias("amazon-sqs-producer")
@AdapterComponent
@ComponentProfile(summary = "Send messages to Amazon SQS", tag = "producer,amazon,sqs",
    recommended = {AmazonSQSConnection.class})
public class AmazonSQSProducer extends ProduceOnlyProducerImp {

  private int delaySeconds = 0;
  
  private transient Map<String, String> cachedQueueURLs = new ConcurrentHashMap<String, String>();
  
  @XStreamImplicit(itemFieldName = "send-attribute")
  private List<String> sendAttributes;

  @AdvancedConfig
  private String ownerAwsAccountId;
  
  public AmazonSQSProducer() {
    sendAttributes = new ArrayList<>();
  }

  @Override
  public void init() throws CoreException {
    if(retrieveConnection(AmazonSQSConnection.class) == null) {
      throw new CoreException("AmazonSQSConnection is required");
    }
    
    if(getDestination() == null) {
      throw new CoreException("Destination is required");
    }
  }
  
  private AmazonSQSAsync getSQS() throws CoreException {
    return retrieveConnection(AmazonSQSConnection.class).getASyncClient();
  }

  @Override
  public void start() throws CoreException {
  }

  @Override
  public void stop() {
  }

  @Override
  public void close() {
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
      getSQS().sendMessageAsync(sendMessageRequest);
    }
    catch (Exception e) {
      throw new ProduceException(e);
    }
  }

  private void applyMetadata(SendMessageRequest sendMessageRequest, AdaptrisMessage msg) {
    if((this.getSendAttributes() != null) && (this.getSendAttributes().size() > 0)) {
      Map<String, MessageAttributeValue> attributes = new HashMap<>();
      for(String attribute : this.getSendAttributes()) {
        String adpMetadataValue = msg.getMetadataValue(attribute);
        if((adpMetadataValue != null) && (!adpMetadataValue.equals(""))) {
          MessageAttributeValue mav = new MessageAttributeValue();
          mav.setDataType("String");
          mav.setStringValue(msg.getMetadataValue(attribute));
          attributes.put(attribute, mav);
        }
      }
      
      sendMessageRequest.withMessageAttributes(attributes);
    }
  }
  
  private String resolveQueueURL(AdaptrisMessage msg) throws AmazonServiceException, AmazonClientException, CoreException {
    // Get destination (possibly from message)
    final String queueName = getDestination().getDestination(msg);

    String queueURL = cachedQueueURLs.get(queueName);
    
    // It's not in the cache. Look up the queue url from Amazon and cache it.
    if(queueURL == null) {
      queueURL = retrieveQueueURLFromSQS(queueName);
      cachedQueueURLs.put(queueName, queueURL);
    }

    return queueURL;
  }
  
  private String retrieveQueueURLFromSQS(String queueName) throws AmazonServiceException, AmazonClientException, CoreException {
    GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
    if (!StringUtils.isEmpty(getOwnerAwsAccountId())) {
      getQueueUrlRequest.withQueueOwnerAWSAccountId(getOwnerAwsAccountId());
    }
    return getSQS().getQueueUrl(getQueueUrlRequest).getQueueUrl();
  }

  @Override
  public void prepare() throws CoreException {
  }

  /**
   * Delay seconds for every message
   * 
   * @return delaySeconds
   */
  public int getDelaySeconds() {
    return delaySeconds;
  }

  /**
   * Delay seconds for every message
   * 
   * @param delaySeconds
   */
  public void setDelaySeconds(int delaySeconds) {
    this.delaySeconds = delaySeconds;
  }

  public List<String> getSendAttributes() {
    return sendAttributes;
  }

  /**
   * Specify a list of a metadata keys that should be attached to a message.
   * <p>
   * Amazon SQS supports a limited set of attributes (10 at current count) that can be attached to a message; use this list to
   * specify the metadata keys that must be sent as attributes, otherwise all metadata is ignored.
   * </p>
   * 
   * @since 3.0.3
   * @param sendAttributes a list of metadata keys that will be sent as attributes.
   */
  public void setSendAttributes(List<String> sendAttributes) {
    this.sendAttributes = sendAttributes;
  }

  public String getOwnerAwsAccountId() {
    return ownerAwsAccountId;
  }

  /**
   * The AWS account ID of the account that created the queue. When omitted
   * the default setting on the queue will be used.
   * @since 3.7.3
   * @param ownerAwsAccountId
   */
  public void setOwnerAwsAccountId(String ownerAwsAccountId) {
    this.ownerAwsAccountId = ownerAwsAccountId;
  }

}
