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

import static com.adaptris.core.util.DestinationHelper.logWarningIfNotNull;
import static com.adaptris.core.util.DestinationHelper.mustHaveEither;
import static com.adaptris.core.util.DestinationHelper.resolveProduceDestination;
import javax.validation.Valid;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.common.StringPayloadDataInputParameter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.types.InterlokMessage;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
@NoArgsConstructor
@DisplayOrder(order = {"topicArn", "snsSubject"})
public class PublishToTopic extends NotificationProducer {

  /** The metadata that will contain the SNS MessageID post produce.
   *
   */
  public static final String SNS_MSG_ID_KEY = "SNS_MessageID";

  private static final DataInputParameter<String> DEFAULT_SOURCE = new StringPayloadDataInputParameter();
  private static final DataInputParameter<String> EMPTY = new DataInputParameter<String>() {

    @Override
    public String extract(InterlokMessage m) throws InterlokException {
      return "";
    }

  };

  @AutoPopulated
  @Valid
  @InputFieldDefault(value = "payload contents")
  @AdvancedConfig
  private DataInputParameter<String> source;

  /**
   * Optional subject that can be specified.
   *
   * @deprecated since 3.11.0 use sns-subject instead.
   */
  @AutoPopulated
  @Valid
  @InputFieldDefault(value = "")
  @AdvancedConfig
  @Deprecated
  @Getter
  @Setter
  private DataInputParameter<String> subject;

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
   * The destination is the topic
   *
   * @deprecated since 3.11.0 use 'topic-arn' instead
   */
  @Getter
  @Setter
  @Deprecated
  @Valid
  @Removal(version = "4.0.0", message = "Use 'topic-arn' instead")
  private ProduceDestination destination;

  private transient boolean destWarning;
  private transient boolean subjectWarning;


  @Override
  public void prepare() throws CoreException {
    logWarningIfNotNull(destWarning, () -> destWarning = true, getDestination(),
        "{} uses destination, use 'topic-arn' instead", LoggingHelper.friendlyName(this));
    logWarningIfNotNull(subjectWarning, () -> subjectWarning = true, getSubject(),
        "{} uses destination, use 'sns-subject' instead", LoggingHelper.friendlyName(this));
    mustHaveEither(getTopicArn(), getDestination());
  }

  public DataInputParameter<String> getSource() {
    return source;
  }

  /**
   * St the contents of the message that will be published.
   *
   * @param source the message contents; by default will the be payload of the message.
   */
  public void setSource(DataInputParameter<String> source) {
    this.source = source;
  }

  public <T extends PublishToTopic> T withSource(DataInputParameter<String> source) {
    setSource(source);
    return (T)this;
  }

  @Deprecated
  @Removal(version = "4.0.0")
  public <T extends PublishToTopic> T withSubject(DataInputParameter<String> subject) {
    setSubject(subject);
    return (T)this;
  }

  public <T extends PublishToTopic> T withTopicArn(String s) {
    setTopicArn(s);
    return (T) this;
  }

  public <T extends PublishToTopic> T withSubject(String s) {
    setSnsSubject(s);
    return (T) this;
  }

  protected DataInputParameter<String> source() {
    return ObjectUtils.defaultIfNull(getSource(), DEFAULT_SOURCE);
  }

  protected String resolveSubject(AdaptrisMessage msg) throws Exception {
    if (getSubject() != null) {
      return getSubject().extract(msg);
    }
    return msg.resolve(getSnsSubject());
  }


  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      PublishRequest request = new PublishRequest(endpoint, source().extract(msg));
      String subject = resolveSubject(msg);
      if (!StringUtils.isBlank(subject)) {
        request.setSubject(subject);
      }
      PublishResult result = client().publish(request);
      msg.addMetadata(SNS_MSG_ID_KEY, result.getMessageId());
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return resolveProduceDestination(getTopicArn(), getDestination(), msg);
  }

}
