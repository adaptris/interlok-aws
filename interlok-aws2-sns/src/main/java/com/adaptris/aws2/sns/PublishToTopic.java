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

package com.adaptris.aws2.sns;

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
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.types.InterlokMessage;
import com.adaptris.interlok.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import static com.adaptris.core.util.DestinationHelper.resolveProduceDestination;

/**
 * Publish a message to the SNS topic.
 * <p>
 * The associated destination should be the topic ARN (e.g. {@code arn:aws2:sns:us-east-1:123456789012:MyNewTopic}). It is expected
 * that you have previously created the topic already, either via the AWS CLI or some other means.
 * </p>
 *
 * <p>
 * By default the messageID of the message published to the SNS topic will be stored against the key {@code SNS_MessageID}.
 *
 * @config amazon-sns-topic-publisher
 * @since 4.3.0
 *
 */
@XStreamAlias("aws2-amazon-sns-topic-publisher")
@ComponentProfile(summary = "Publish a message to an SNS Topic", tag = "producer,amazon,sns", since = "4.3.0", recommended =
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
   * *
   * <p>
   * The topic ARN (e.g. {@code arn:aws2:sns:us-east-1:123456789012:MyNewTopic}). It is expected that
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

  public <T extends PublishToTopic> T withSource(DataInputParameter<String> source) {
    setSource(source);
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
    return msg.resolve(getSnsSubject());
  }


  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      PublishRequest.Builder builder = PublishRequest.builder();
      builder.topicArn(endpoint);
      builder.message(source().extract(msg));
      String subject = resolveSubject(msg);
      if (!StringUtils.isBlank(subject)) {
        builder.subject(subject);
      }
      PublishResponse result = client().publish(builder.build());
      msg.addMetadata(SNS_MSG_ID_KEY, result.messageId());
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return resolveProduceDestination(getTopicArn(), msg);
  }

}
