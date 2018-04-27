package com.adaptris.aws.sns;

import javax.validation.Valid;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.common.StringPayloadDataInputParameter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.types.InterlokMessage;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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
public class PublishToTopic extends NotificationProducer {

  private static final String SNS_MSG_ID_KEY = "SNS_MessageID";

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
  @AutoPopulated
  @Valid
  @InputFieldDefault(value = "")
  @AdvancedConfig
  private DataInputParameter<String> subject;

  public PublishToTopic() {

  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    try {
      PublishRequest request = new PublishRequest(getDestination().getDestination(msg), source().extract(msg),
          subject().extract(msg));
      PublishResult result = client().publish(request);
      msg.addMetadata(SNS_MSG_ID_KEY, result.getMessageId());
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
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

  public DataInputParameter<String> getSubject() {
    return subject;
  }

  /**
   * Optional subject that can be specified.
   * 
   * @param subject the subject
   */
  public void setSubject(DataInputParameter<String> subject) {
    this.subject = subject;
  }

  DataInputParameter<String> source() {
    return getSource() != null ? getSource() : DEFAULT_SOURCE;
  }

  DataInputParameter<String> subject() {
    return getSubject() != null ? getSubject() : EMPTY;
  }
}
