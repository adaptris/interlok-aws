package com.adaptris.aws.kinesis;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.CloseableIterable;
import com.adaptris.util.NumberUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamMode;
import com.amazonaws.services.kinesis.model.StreamModeDetails;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Producer to amazon kinesis using the SDK.
 * <p>
 * This may be the preferred approach over using KPL if you're running in environment where you
 * don't want other processes to be spawned (for example: containerised).
 * </p>
 * 
 * @config aws-kinesis-sdk-stream-producer
 */
@ComponentProfile(summary = "Produce to Amazon Kinesis using the SDK", tag = "amazon,aws,kinesis,producer",
    recommended = {AWSKinesisSDKConnection.class})
@DisplayOrder(order = {"stream", "partitionKey", "batchWindow", "requestBuilder"})
@XStreamAlias("aws-kinesis-sdk-stream-producer")
@NoArgsConstructor
public class KinesisSDKStreamProducer extends ProduceOnlyProducerImp {

  private static final int DEFAULT_BATCH_WINDOW = 100;

  // 0 means ON_DEMAND, > 0 means PROVISIONED
  private static final int SHARD_COUNT_NONE = 0;


  /**
   * The kinesis stream name.
   *
   */
  @NotBlank
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
   * Request Builder enables the control on how the records are put, this is used in conjunction with batchWindow.
   */
  @Getter
  @Setter
  @InputFieldDefault(value = "DefaultRequestBuilder")
  private RequestBuilder requestBuilder;

  /**
   * Batch window controls on how the records are put to the stream, this used in conjunction with a splitting implementation
   * of the request builder.
   */
  @Min(0)
  @InputFieldDefault(value = "100")
  @Getter
  @Setter
  private Integer batchWindow;

  /**
   * Toggles whether to create the stream if it does not exist at the time of sending
   */
  @Getter
  @Setter
  private boolean createIfNotExists = false;

  /**
   * If creating a stream, this specifies the number of shards for StreamMode.PROVISIONED.
   * A value of 0 means StreamMode.ON_DEMAND.
   */
  @Min(SHARD_COUNT_NONE)
  @Getter
  @Setter
  private int shardCount = SHARD_COUNT_NONE;

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getPartitionKey(), "partition-key");
    Args.notBlank(getStream(), "stream");
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

  public <T extends KinesisSDKStreamProducer> T withRequestBuilder(RequestBuilder r) {
    setRequestBuilder(r);
    return (T) this;
  }

  public <T extends KinesisSDKStreamProducer> T withBatchWindow(Integer i) {
    setBatchWindow(i);
    return (T) this;
  }

  public <T extends KinesisSDKStreamProducer> T withCreateIfNotExists(boolean createIfNotExists) {
    setCreateIfNotExists(createIfNotExists);
    return (T) this;
  }

  public <T extends KinesisSDKStreamProducer> T withShardCount(int shardCount) {
    setShardCount(shardCount);
    return (T) this;
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    try {
      AmazonKinesis kinesisClient = retrieveConnection(AWSKinesisSDKConnection.class).kinesisClient();
      long total = 0;
      try (CloseableIterable<PutRecordsRequestEntry> docs = CloseableIterable.ensureCloseable(requestBuilder().build(getPartitionKey(), msg))) {
        int count = 0;
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (PutRecordsRequestEntry putRecordsRequestEntry : docs) {
          count++;
          total++;
          putRecordsRequestEntryList.add(putRecordsRequestEntry);
          if (count >= batchWindow()) {
            doSend(kinesisClient, endpoint, putRecordsRequestEntryList);
            count = 0;
            putRecordsRequestEntryList = new ArrayList<>();
          }
        }
        if (count > 0) {
          doSend(kinesisClient, endpoint, putRecordsRequestEntryList);
        }
      }
      log.debug("Produced a total of {} documents", total);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  // According to the docs, we need to wait for the stream to be active
  // https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-create-stream.html
  private void doAwaitStreamActive(AmazonKinesis kinesisClient, String endpoint) throws ProduceException {

    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(endpoint);

    long startTime = System.currentTimeMillis();
    long endTime = startTime + ( 10 * 60 * 1000 );
    while ( System.currentTimeMillis() < endTime ) {
      try {
        Thread.sleep(20 * 1000);
      }
      catch ( Exception e ) {}

      try {
        DescribeStreamResult describeStreamResponse = kinesisClient .describeStream( describeStreamRequest );
        StreamStatus streamStatus = StreamStatus.fromValue(describeStreamResponse.getStreamDescription().getStreamStatus());
        if ( streamStatus.equals( StreamStatus.ACTIVE ) ) {
          break;
        }
        //
        // sleep for one second
        //
        try {
          Thread.sleep( 1000 );
        }
        catch ( Exception e ) {}
      }
      catch ( ResourceNotFoundException e ) {}
    }
    if ( System.currentTimeMillis() >= endTime ) {
      throw new ProduceException( "Stream " + endpoint + " never went active" );
    }
  }

  private void doCreate(AmazonKinesis kinesisClient, String endpoint, int shardCount) throws ProduceException {
    CreateStreamRequest request = new CreateStreamRequest().withStreamName(endpoint);
    // only if shard count > 0, we assume PROVISIONED, else ON_DEMAND
    if (shardCount > SHARD_COUNT_NONE) {
      request.withStreamModeDetails(new StreamModeDetails().withStreamMode(StreamMode.PROVISIONED))
      .withShardCount(shardCount);
    } else {
      request.withStreamModeDetails(new StreamModeDetails().withStreamMode(StreamMode.ON_DEMAND));
    }
    kinesisClient.createStream(request);

    doAwaitStreamActive(kinesisClient, endpoint);

  }

  private void doSend(AmazonKinesis kinesisClient, String endpoint, List <PutRecordsRequestEntry> putRecordsRequestEntryList) throws ProduceException{
    try {
      PutRecordsRequest putRecordsRequest  = new PutRecordsRequest().withStreamName(endpoint).withRecords(putRecordsRequestEntryList);
      PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
      log.trace("PutRecordResults: {}", putRecordsResult);
    } catch (ResourceNotFoundException rnfex) {
      if (createIfNotExists) {
        log.debug("Creating stream as it does not exist");
        doCreate(kinesisClient, endpoint, shardCount);
        log.debug("Resending records to newly created stream");
        doSend(kinesisClient, endpoint, putRecordsRequestEntryList);
      } else {
        throw new ProduceException(rnfex);
      }
    }
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getStream());
  }

  RequestBuilder requestBuilder(){
    return getRequestBuilder() != null ? getRequestBuilder() : new DefaultRequestBuilder();
  }

  private int batchWindow() {
    return NumberUtils.toIntDefaultIfNull(getBatchWindow(), DEFAULT_BATCH_WINDOW);
  }

}
