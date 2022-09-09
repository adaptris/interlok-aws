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

package com.adaptris.aws.sns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.common.StringPayloadDataInputParameter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.util.Args;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Publish a message to the SNS topic.
 * <p>
 * The associated destination should be the topic ARN (e.g. {@code arn:aws:sns:us-east-1:123456789012:MyNewTopic}). It is expected
 * that you have previously created the topic already, either via the AWS CLI or some other means.
 * </p>
 *
 * <p>
 * By default the messageID of the message published to the SNS topic will be stored against the key {@code SNS_MessageID}.
 *
 * @config amazon-sns-topic-publisher
 *
 */
@XStreamAlias("amazon-sns-topic-publisher")
@ComponentProfile(summary = "Publish a message to an SNS Topic", tag = "producer,amazon,sns", recommended =
{
    AmazonSNSConnection.class
}, metadata =
  {
      "SNS_MessageID"
  })
@DisplayOrder(order = {"topicArn", "snsSubject"})
public class PublishToTopic extends NotificationProducer {

  /** The metadata that will contain the SNS MessageID post produce.
   *
   */
  public static final String SNS_MSG_ID_KEY = "SNS_MessageID";

  private static final DataInputParameter<String> DEFAULT_SOURCE = new StringPayloadDataInputParameter();

  @AutoPopulated
  @Valid
  @InputFieldDefault(value = "payload contents")
  @AdvancedConfig
  private DataInputParameter<String> source;

  /**
   * *
   * <p>
   * The topic ARN (e.g. {@code arn:aws:sns:us-east-1:123456789012:MyNewTopic}). It is expected that
   * you have previously created the topic already, either via the AWS CLI or some other means.
   * </p>
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NotBlank
  private String topicArn;

  /**
   * Optional subject that can be specified.
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String snsSubject;

  /**
   * Specify a list of a metadata keys that should be attached to a message.
   * <p>
   * Amazon SNS supports a limited set of attributes (10 at current count) that can be attached to a message; use this list to specify the
   * metadata keys that must be sent as attributes, otherwise all metadata is ignored.
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
   * <p>
   * This parameter applies only to FIFO (first-in-first-out) topics. The <code>MessageGroupId</code> can contain up to 128 alphanumeric
   * characters <code>(a-z, A-Z, 0-9)</code> and punctuation <code>(!"#$%&amp;'()*+,-./:;&lt;=&gt;?@[\]^_`{|}~)</code>.
   * </p>
   * <p>
   * The <code>MessageGroupId</code> is a tag that specifies that a message belongs to a specific message group. Messages that belong to the
   * same message group are processed in a FIFO manner (however, messages in different message groups might be processed out of order).
   * Every message must include a <code>MessageGroupId</code>.
   * </p>
   *
   * @param messageGroupId
   * @return Returns a reference to this object so that method calls can be chained together.
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @Size(max = 128)
  private String messageGroupId;

  public PublishToTopic() {
    setSendAttributes(new ArrayList<String>());
  }

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getTopicArn(), "topic-arn");
  }

  public DataInputParameter<String> getSource() {
    return source;
  }

  /**
   * The contents of the message that will be published.
   *
   * @param source the message contents; by default will the be payload of the message.
   */
  public void setSource(DataInputParameter<String> source) {
    this.source = source;
  }

  public PublishToTopic withSource(DataInputParameter<String> source) {
    setSource(source);
    return this;
  }

  public PublishToTopic withTopicArn(String s) {
    setTopicArn(s);
    return this;
  }

  public PublishToTopic withSubject(String s) {
    setSnsSubject(s);
    return this;
  }

  protected DataInputParameter<String> source() {
    return ObjectUtils.defaultIfNull(getSource(), DEFAULT_SOURCE);
  }

  protected String subject(AdaptrisMessage msg) throws Exception {
    return msg.resolve(getSnsSubject());
  }

  protected String messageGroupId(AdaptrisMessage msg) throws Exception {
    return msg.resolve(getMessageGroupId());
  }


  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      PublishRequest request = new PublishRequest(endpoint, source().extract(msg)).withMessageGroupId(messageGroupId(msg));
      String subject = subject(msg);
      if (!StringUtils.isBlank(subject)) {
        request.setSubject(subject);
      }
      applyMetadata(request, msg);
      PublishResult result = client().publish(request);
      msg.addMetadata(SNS_MSG_ID_KEY, result.getMessageId());
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getTopicArn());
  }

  private PublishRequest applyMetadata(PublishRequest request, AdaptrisMessage msg) {
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

}
