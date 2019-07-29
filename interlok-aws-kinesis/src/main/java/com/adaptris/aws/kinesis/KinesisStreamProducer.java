package com.adaptris.aws.kinesis;

import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

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
 */
@ComponentProfile(summary = "Produce to Amazon Kinesis using the Kinesis Producer Library", tag = "amazon,aws,kinesis,producer",
    recommended = {ProducerLibraryConnection.class})
public class KinesisStreamProducer extends ProduceOnlyProducerImp {

  @InputFieldHint(expression = true)
  private String stream;
  @NotBlank
  @InputFieldHint(expression = true)
  private String partitionKey;

  public KinesisStreamProducer() {

  }

  @Override
  public void prepare() throws CoreException {
  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    try {
      KinesisProducer producer = retrieveConnection(KinesisProducerWrapper.class).kinesisProducer();
      String myStream = getStream(msg, destination);
      String myPartitionKey = msg.resolve(getPartitionKey());
      producer.addUserRecord(myStream, myPartitionKey, ByteBuffer.wrap(msg.getPayload()));
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  private String getStream(AdaptrisMessage msg, ProduceDestination destination) throws CoreException {
    if (destination != null) {
      return StringUtils.defaultIfBlank(msg.resolve(getStream()), destination.getDestination(msg));
    }
    // ProduceDestination is null, so we have to use what is available.
    return msg.resolve(getStream());

  }
  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public KinesisStreamProducer withStream(String s) {
    setStream(s);
    return this;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(String partitionKey) {
    this.partitionKey = Args.notBlank(partitionKey, "partition-key");
  }

  public KinesisStreamProducer withPartitionKey(String s) {
    setPartitionKey(s);
    return this;
  }

}
