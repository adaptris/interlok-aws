package com.adaptris.aws.kinesis;

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
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;

/**
 * Producer to amazon kinesis using the SDK.
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

  private transient boolean destWarning;

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

  private void doSend(AmazonKinesis kinesisClient, String endpoint, List <PutRecordsRequestEntry> putRecordsRequestEntryList){
    PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
    putRecordsRequest.setStreamName(endpoint);
    putRecordsRequest.setRecords(putRecordsRequestEntryList);
    PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
    log.trace("PutRecordResults: {}", putRecordsResult);
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
