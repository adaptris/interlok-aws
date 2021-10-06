package com.adaptris.aws.kinesis;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFutureTask;

public class KinesisSynchronousStreamProducerTest extends ExampleProducerCase {

  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    KinesisSynchronousStreamProducer producer =
        new KinesisSynchronousStreamProducer().withStream("%message{myStreamName}").withPartitionKey("myPartitionKey");
    ConnectionFromProperties conn = new ConnectionFromProperties().withConfigLocation("/path/to/property/file");
    return new StandaloneProducer(conn, producer);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduce_NoDestination() throws Exception {
    KinesisSynchronousStreamProducer producer = new KinesisSynchronousStreamProducer().withStream("myStreamName").withPartitionKey("myPartitionKey");
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> futureTask = Mockito.mock(ListenableFutureTask.class);
    Mockito.when(futureTask.get()).thenReturn(mockResult);
    Mockito.when(mockResult.isSuccessful()).thenReturn(true);
    List<Attempt> list = Collections.singletonList(new Attempt(0, 0, "Success", "Success", true));
    Mockito.when(mockResult.getAttempts()).thenReturn(list);
    Mockito.when(mock.addUserRecord(anyString(), anyString(), any())).thenReturn(futureTask);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(standalone, msg);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduce_Exception() throws Exception {
    KinesisSynchronousStreamProducer producer =
        new KinesisSynchronousStreamProducer().withStream("%message{does not exist}").withPartitionKey("%message{does not exist}");
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> futureTask = Mockito.mock(ListenableFutureTask.class);
    Mockito.when(futureTask.get()).thenReturn(mockResult);
    Mockito.doThrow(new IllegalArgumentException()).when(mock).addUserRecord(anyString(),
        anyString(), any());
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(standalone, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduce_Unsuccessful_WithException() throws Exception {
    KinesisSynchronousStreamProducer producer = new KinesisSynchronousStreamProducer().withStream("myStreamName").withPartitionKey("myPartitionKey");
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> futureTask = Mockito.mock(ListenableFutureTask.class);
    Mockito.doThrow(new ExecutionException("Mocked Exception", new UserRecordFailedException(mockResult))).when(futureTask).get();
    Mockito.when(mockResult.isSuccessful()).thenReturn(false);
    List<Attempt> list = Collections.singletonList(new Attempt(0, 0, "Failed", "Failed", false));
    Mockito.when(mockResult.getAttempts()).thenReturn(list);
    Mockito.when(mock.addUserRecord(anyString(), anyString(), any())).thenReturn(futureTask);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(standalone, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testProduce_Unsuccessful_WithOutException() throws Exception {
    KinesisSynchronousStreamProducer producer = new KinesisSynchronousStreamProducer().withStream("myStreamName").withPartitionKey("myPartitionKey");
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> futureTask = Mockito.mock(ListenableFutureTask.class);
    Mockito.when(futureTask.get()).thenReturn(mockResult);
    Mockito.when(mockResult.isSuccessful()).thenReturn(false);
    List<Attempt> list = Collections.singletonList(new Attempt(0, 0, "Failed", "Failed", false));
    Mockito.when(mockResult.getAttempts()).thenReturn(list);
    Mockito.when(mock.addUserRecord(anyString(), anyString(), any())).thenReturn(futureTask);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(standalone, msg);
  }

  private class MyConnection extends ProducerLibraryConnection {

    private MyConnection(KinesisProducer p) {
      producer = p;
    }

    @Override
    public KinesisProducer kinesisProducer() throws Exception {
      return producer;
    }

  }
}
