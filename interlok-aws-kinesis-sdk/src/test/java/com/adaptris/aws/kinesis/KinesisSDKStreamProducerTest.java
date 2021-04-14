package com.adaptris.aws.kinesis;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.services.splitter.LineCountSplitter;
import com.adaptris.core.services.splitter.NoOpSplitter;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.any;

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
    AmazonKinesis mock = Mockito.mock(AmazonKinesis.class);
    Mockito.doThrow(new ResourceNotFoundException("Error [does not exist] does not exist")).when(mock).putRecords(any());
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(standalone, msg);
      fail();
    } catch (ServiceException expected) {
      Mockito.verify(mock, Mockito.times(1)).putRecords(any());
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
    AmazonKinesis mock = Mockito.mock(AmazonKinesis.class);
    PutRecordsResult mockResult = Mockito.mock(PutRecordsResult.class);

    ArgumentCaptor<PutRecordsRequest> argumentCaptor = ArgumentCaptor.forClass(PutRecordsRequest.class);
    Mockito.when(mock.putRecords(argumentCaptor.capture())).thenReturn(mockResult);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.setContent("Record 1\nRecord 2", StandardCharsets.UTF_8.name());

    ExampleServiceCase.execute(standalone, msg);

    Mockito.verify(mock, Mockito.times(2)).putRecords(any());

    List<PutRecordsRequest> putRecordsRequest = argumentCaptor.getAllValues();
    assertEquals(2, putRecordsRequest.size());

    assertEquals(1, putRecordsRequest.get(0).getRecords().size());
    assertEquals("Record 1\n", StandardCharsets.UTF_8.decode(putRecordsRequest.get(0).getRecords().get(0).getData()).toString());
    assertEquals(1, putRecordsRequest.get(1).getRecords().size());
    assertEquals("Record 2\n", StandardCharsets.UTF_8.decode(putRecordsRequest.get(1).getRecords().get(0).getData()).toString());
  }

  private void runTest(KinesisSDKStreamProducer producer, List<String> results) throws Exception{
    AmazonKinesis mock = Mockito.mock(AmazonKinesis.class);
    PutRecordsResult mockResult = Mockito.mock(PutRecordsResult.class);

    ArgumentCaptor<PutRecordsRequest> argumentCaptor = ArgumentCaptor.forClass(PutRecordsRequest.class);
    Mockito.when(mock.putRecords(argumentCaptor.capture())).thenReturn(mockResult);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.setContent("Record 1\nRecord 2", StandardCharsets.UTF_8.name());

    ExampleServiceCase.execute(standalone, msg);

    Mockito.verify(mock, Mockito.times(1)).putRecords(any());

    PutRecordsRequest putRecordsRequest = argumentCaptor.getValue();
    assertEquals(results.size(), putRecordsRequest.getRecords().size());

    int i = 0;
    for (String expected : results) {
      assertEquals(expected, StandardCharsets.UTF_8.decode(putRecordsRequest.getRecords().get(i++).getData()).toString());
    }
  }

  private static class MyConnection extends AWSKinesisSDKConnection {

    private final transient AmazonKinesis producer;

    private MyConnection(AmazonKinesis p) {
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
    public AmazonKinesis kinesisClient() {
      return producer;
    }

  }
}
