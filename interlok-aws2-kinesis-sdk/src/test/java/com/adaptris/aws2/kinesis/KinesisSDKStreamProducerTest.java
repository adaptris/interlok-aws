package com.adaptris.aws2.kinesis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.ProduceException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.services.splitter.LineCountSplitter;
import com.adaptris.core.services.splitter.NoOpSplitter;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamMode;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

public class KinesisSDKStreamProducerTest extends ExampleProducerCase {


  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    KinesisSDKStreamProducer producer =
        new KinesisSDKStreamProducer().withStream("%message{myStreamName}").withPartitionKey("myPartitionKey");
    AWSKinesisSDKConnection conn = new AWSKinesisSDKConnection();
    conn.setCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
    conn.setRegion("My AWS Region");
    return new StandaloneProducer(conn, producer);
  }

  @Test
  public void testProduce_NoDestination() throws Exception {
    KinesisSDKStreamProducer producer = new KinesisSDKStreamProducer()
      .withStream("myStreamName")
      .withPartitionKey("myPartitionKey");

    runTest(producer, Collections.singletonList("Record 1\nRecord 2"));
  }

  @Test
  public void testProduce_Exception() throws Exception {
    KinesisSDKStreamProducer producer =
        new KinesisSDKStreamProducer()
          .withStream("%message{does not exist}")
          .withPartitionKey("%message{does not exist}");
    KinesisClient mock = Mockito.mock(KinesisClient.class);
    Mockito.doThrow(ResourceNotFoundException.builder().message("Error [does not exist] does not exist").build()).when(mock).putRecords((PutRecordsRequest) any());
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(standalone, msg);
      fail();
    } catch (ServiceException expected) {
      Mockito.verify(mock, Mockito.times(1)).putRecords((PutRecordsRequest) any());
    }
  }

  @Test
  public void testProduceNoOpSplitter() throws Exception {
    KinesisSDKStreamProducer producer =
      new KinesisSDKStreamProducer()
        .withPartitionKey("myPartitionKey")
        .withStream("myStreamName")
        .withRequestBuilder(new SplittingRequestBuilder().withMessageSplitter(new NoOpSplitter()));

    runTest(producer, Collections.singletonList("Record 1\nRecord 2"));
  }

  @Test
  public void testProduceLineCountSplitter() throws Exception {
    LineCountSplitter lineCountSplitter = new LineCountSplitter();
    lineCountSplitter.setSplitOnLine(1);
    KinesisSDKStreamProducer producer =
      new KinesisSDKStreamProducer()
        .withPartitionKey("myPartitionKey")
        .withStream("myStreamName")
        .withRequestBuilder(new SplittingRequestBuilder().withMessageSplitter(lineCountSplitter));
    runTest(producer, Arrays.asList("Record 1\n", "Record 2\n"));
  }

  @Test
  public void testProduceLineCountSplitterBatchSize() throws Exception {
    LineCountSplitter lineCountSplitter = new LineCountSplitter();
    lineCountSplitter.setSplitOnLine(1);
    KinesisSDKStreamProducer producer =
      new KinesisSDKStreamProducer()
        .withPartitionKey("myPartitionKey")
        .withStream("myStreamName")
        .withRequestBuilder(new SplittingRequestBuilder().withMessageSplitter(lineCountSplitter))
        .withBatchWindow(1);
    KinesisClient mock = Mockito.mock(KinesisClient.class);
    PutRecordsResponse mockResult = Mockito.mock(PutRecordsResponse.class);

    ArgumentCaptor<PutRecordsRequest> argumentCaptor = ArgumentCaptor.forClass(PutRecordsRequest.class);
    Mockito.when(mock.putRecords(argumentCaptor.capture())).thenReturn(mockResult);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.setContent("Record 1\nRecord 2", StandardCharsets.UTF_8.name());

    ExampleServiceCase.execute(standalone, msg);

    Mockito.verify(mock, Mockito.times(2)).putRecords((PutRecordsRequest) any());

    List<PutRecordsRequest> putRecordsRequest = argumentCaptor.getAllValues();
    assertEquals(2, putRecordsRequest.size());

    assertEquals(1, putRecordsRequest.get(0).records().size());
    // Dodgy windows \r\n so we normalize and trim
    assertEquals("Record 1", StringUtils.normalizeSpace(StandardCharsets.UTF_8
        .decode(putRecordsRequest.get(0).records().get(0).data().asByteBuffer()).toString()).trim());
    assertEquals(1, putRecordsRequest.get(1).records().size());
    assertEquals("Record 2", StringUtils.normalizeSpace(StandardCharsets.UTF_8
        .decode(putRecordsRequest.get(1).records().get(0).data().asByteBuffer()).toString()).trim());
  }

  @Test
  public void testCreateIfNotExists() throws Exception {
    KinesisSDKStreamProducer producer = new KinesisSDKStreamProducer()
            .withStream("myStreamName")
            .withPartitionKey("myPartitionKey")
            .withShardCount(KinesisSDKStreamProducer.SHARD_COUNT_NONE)
            .withCreateStreamPollTimeMillis(100)
            .withCreateStreamMaxWaitTimeMillis(500);

    KinesisClient mock = Mockito.mock(KinesisClient.class);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    ArgumentCaptor<PutRecordsRequest> argumentCaptor = ArgumentCaptor.forClass(PutRecordsRequest.class);

    DescribeStreamResponse describeStreamResponseActive = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder()
                    .streamStatus(StreamStatus.ACTIVE)
                    .build())
            .build();
    ArgumentCaptor<CreateStreamRequest> createStreamRequestCaptor = ArgumentCaptor.forClass(CreateStreamRequest.class);
    ArgumentCaptor<DescribeStreamRequest> describeStreamRequestCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();

    ResourceNotFoundException rnfex = ResourceNotFoundException.builder().message("Error [does not exist] does not exist").build();
    Mockito.when(mock.putRecords(argumentCaptor.capture()))
            .thenThrow(rnfex) // 1st doService()
            .thenThrow(rnfex)
            .thenReturn(Mockito.mock(PutRecordsResponse.class)) // 2nd doService()
            .thenThrow(rnfex)
            .thenReturn(Mockito.mock(PutRecordsResponse.class)); // 3rd doService()
    Mockito.when(mock.describeStream(describeStreamRequestCaptor.capture())).thenReturn(describeStreamResponseActive);
    Mockito.when(mock.createStream(createStreamRequestCaptor.capture())).thenReturn(Mockito.mock(CreateStreamResponse.class));

    start(standalone);
    try {
      // when createIfNotExists is false, exception is thrown
      producer.withCreateIfNotExists(false);

      ServiceException thrown = assertThrows(ServiceException.class, () -> standalone.doService(msg));
      assertEquals(ProduceException.class, thrown.getCause().getClass());
      assertEquals(ResourceNotFoundException.class, thrown.getCause().getCause().getClass());

      // when createIfNotExists is true and shard count is 0, createStream() and describeStream() are called
      // with StreamMode.ON_DEMAND
      AdaptrisMessage msg1 = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      producer.setCreateIfNotExists(true);
      standalone.doService(msg1);

      Mockito.verify(mock, Mockito.times(1)).createStream(any(CreateStreamRequest.class));
      Mockito.verify(mock, Mockito.times(1)).describeStream(any(DescribeStreamRequest.class));
      assertEquals(StreamMode.ON_DEMAND, createStreamRequestCaptor.getValue().streamModeDetails().streamMode());
      assertNull(createStreamRequestCaptor.getValue().shardCount());

      // when createIfNotExists is true and shard count > 0, createStream() and describeStream() are called
      // with StreamMode.PROVISIONED
      AdaptrisMessage msg2 = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      producer.setShardCount(1);
      standalone.doService(msg2);

      Mockito.verify(mock, Mockito.times(2)).createStream(any(CreateStreamRequest.class));
      Mockito.verify(mock, Mockito.times(2)).describeStream(any(DescribeStreamRequest.class));
      assertEquals(StreamMode.PROVISIONED, createStreamRequestCaptor.getValue().streamModeDetails().streamMode());
      assertEquals(producer.getShardCount(), createStreamRequestCaptor.getValue().shardCount());
    } finally {
      stop(standalone);
    }
  }

  private void runTest(KinesisSDKStreamProducer producer, List<String> results) throws Exception{
    KinesisClient mock = Mockito.mock(KinesisClient.class);
    PutRecordsResponse mockResult = Mockito.mock(PutRecordsResponse.class);

    ArgumentCaptor<PutRecordsRequest> argumentCaptor = ArgumentCaptor.forClass(PutRecordsRequest.class);
    Mockito.when(mock.putRecords(argumentCaptor.capture())).thenReturn(mockResult);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.setContent("Record 1\nRecord 2", StandardCharsets.UTF_8.name());

    ExampleServiceCase.execute(standalone, msg);

    Mockito.verify(mock, Mockito.times(1)).putRecords((PutRecordsRequest) any());

    PutRecordsRequest putRecordsRequest = argumentCaptor.getValue();
    assertEquals(results.size(), putRecordsRequest.records().size());

    int i = 0;
    for (String s : results) {
      // Dodgy windows \r\n so we normalize and trim
      String expected = StringUtils.normalizeSpace(s).trim();
      String actual = StringUtils.normalizeSpace(StandardCharsets.UTF_8
          .decode(putRecordsRequest.records().get(i++).data().asByteBuffer()).toString()).trim();
      assertEquals(expected, actual);
    }
  }

  private static class MyConnection extends AWSKinesisSDKConnection {

    private final transient KinesisClient producer;

    private MyConnection(KinesisClient p) {
      producer = p;
    }

    @Override
    protected void initConnection() throws CoreException {
      //skip
    }

    @Override
    protected void stopConnection() {
      //skip
    }

    @Override
    public KinesisClient kinesisClient() {
      return producer;
    }

  }
}
