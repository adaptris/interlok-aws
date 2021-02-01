package com.adaptris.aws.kinesis;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.validation.constraints.ConfigDeprecated;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import java.nio.ByteBuffer;

import static com.adaptris.core.util.DestinationHelper.logWarningIfNotNull;
import static com.adaptris.core.util.DestinationHelper.mustHaveEither;
import static com.adaptris.core.util.DestinationHelper.resolveProduceDestination;

/**
 * Producer to amazon kinesis using the SDK.
 * <p>
 * This departs from a standard producer in the sense the {@link #getDestination()} can be regarded as optional. The reason
 * for this is that both {@code stream} and {@code partitionKey} are required elements, but {@link #getDestination()} only provides
 * a single method (of course we could change it to provide more). So the behaviour here is changed so that
 * <ul>
 * <li>if {@link #setStream(String)} is blank, then we use {@link #getDestination()}, otherwise
 * we use {@link #getStream()}.</li>
 * <li>{@link #setPartitionKey(String)} should always be populated with a non-blank value, which will be used.</li>
 * </ul>
 * </p>
 *
 * @config aws-kinesis-sdk-stream-producer
 */
@ComponentProfile(summary = "Produce to Amazon Kinesis using the SDK", tag = "amazon,aws,kinesis,producer",
    recommended = {AWSKinesisSDKConnection.class})
@DisplayOrder(order = {"stream", "partitionKey"})
@XStreamAlias("aws-kinesis-sdk-stream-producer")
@NoArgsConstructor
public class KinesisSDKStreamProducer extends ProduceOnlyProducerImp {

  /**
   * The kinesis stream name.
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String stream;
  /**
   * The kinesis partition key.
   *
   */
  @NotBlank
  @InputFieldHint(expression = true)
  @Getter
  private String partitionKey;

  /**
   * The ProduceDestination is the stream that we will used.
   *
   * @deprecated since 3.11.0 use 'stream' instead.
   *
   */
  @Getter
  @Setter
  @Deprecated
  @ConfigDeprecated(removalVersion = "4.0.0", message = "use 'stream' instead", groups = Deprecated.class)
  private ProduceDestination destination;

  private transient boolean destWarning;

  @Override
  public void prepare() throws CoreException {
    logWarningIfNotNull(destWarning, () -> destWarning = true, getDestination(),
        "{} uses destination, use 'stream' instead", LoggingHelper.friendlyName(this));
    mustHaveEither(getStream(), getDestination());
  }

  public <T extends KinesisSDKStreamProducer> T withStream(String s) {
    setStream(s);
    return (T) this;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = Args.notBlank(partitionKey, "partition-key");
  }

  public <T extends KinesisSDKStreamProducer> T withPartitionKey(String s) {
    setPartitionKey(s);
    return (T) this;
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      PutRecordRequest putRecordRequest  = new PutRecordRequest();
      putRecordRequest.setStreamName(endpoint);
      putRecordRequest.setData(ByteBuffer.wrap(msg.getPayload()));
      putRecordRequest.setPartitionKey(getPartitionKey());
      AmazonKinesis kinesisClient = retrieveConnection(AWSKinesisSDKConnection.class).kinesisClient();
      PutRecordResult putRecordsResult  = kinesisClient.putRecord(putRecordRequest);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return resolveProduceDestination(getStream(), getDestination(), msg);
  }

}
