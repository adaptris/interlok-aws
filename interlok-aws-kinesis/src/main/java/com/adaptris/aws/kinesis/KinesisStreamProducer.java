package com.adaptris.aws.kinesis;

import static com.adaptris.core.util.DestinationHelper.resolveProduceDestination;
import java.nio.ByteBuffer;
import javax.validation.constraints.NotBlank;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Producer to amazon kinesis using the Kinesis Producer Library.
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
 * @config aws-kinesis-stream-producer
 */
@ComponentProfile(summary = "Produce to Amazon Kinesis using the Kinesis Producer Library", tag = "amazon,aws,kinesis,producer",
    recommended = {ProducerLibraryConnection.class})
@DisplayOrder(order = {"stream", "partitionKey"})
@XStreamAlias("aws-kinesis-stream-producer")
@NoArgsConstructor
public class KinesisStreamProducer extends ProduceOnlyProducerImp {

  /**
   * The kinesis stream name.
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NotBlank
  private String stream;
  /**
   * The kinesis partition key.
   *
   */
  @NotBlank
  @InputFieldHint(expression = true)
  @Getter
  private String partitionKey;

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getPartitionKey(), "partition-key");
    Args.notBlank(getStream(), "stream");
  }

  ListenableFuture<UserRecordResult> addUserRecord(AdaptrisMessage msg, String endpoint)
      throws Exception {
    KinesisProducer producer = retrieveConnection(KinesisProducerWrapper.class).kinesisProducer();
    String myPartitionKey = msg.resolve(getPartitionKey());
    return producer.addUserRecord(endpoint, myPartitionKey, ByteBuffer.wrap(msg.getPayload()));
  }

  public <T extends KinesisStreamProducer> T withStream(String s) {
    setStream(s);
    return (T) this;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = Args.notBlank(partitionKey, "partition-key");
  }

  public <T extends KinesisStreamProducer> T withPartitionKey(String s) {
    setPartitionKey(s);
    return (T) this;
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      addUserRecord(msg, endpoint);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return resolveProduceDestination(getStream(), msg);
  }

}
