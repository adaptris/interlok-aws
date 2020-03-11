package com.adaptris.aws.kinesis;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFutureTask;

public class KinesisStreamProducerTest extends ProducerCase {
  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    KinesisStreamProducer producer =
        new KinesisStreamProducer().withStream("%message{myStreamName}").withPartitionKey("myPartitionKey");
    ConnectionFromProperties conn = new ConnectionFromProperties().withConfigLocation("/path/to/property/file");
    return new StandaloneProducer(conn, producer);
  }

  @Test
  public void testProduce() throws Exception {
    KinesisStreamProducer producer =
        new KinesisStreamProducer().withPartitionKey("myPartitionKey");
    producer.setDestination(new ConfiguredProduceDestination("myStreamName"));
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> future = ListenableFutureTask.create(() -> {
      return mockResult;
    });
    Mockito.when(mock.addUserRecord(anyString(), anyString(), anyObject())).thenReturn(future);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(standalone, msg);
  }

  @Test
  public void testProduce_NoDestination() throws Exception {
    KinesisStreamProducer producer = new KinesisStreamProducer().withStream("myStreamName").withPartitionKey("myPartitionKey");
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> future = ListenableFutureTask.create(() -> {
      return mockResult;
    });
    Mockito.when(mock.addUserRecord(anyString(), anyString(), anyObject())).thenReturn(future);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(standalone, msg);
  }

  @Test
  public void testProduce_Exception() throws Exception {
    KinesisStreamProducer producer =
        new KinesisStreamProducer().withStream("%message{does not exist}").withPartitionKey("%message{does not exist}");
    KinesisProducer mock = Mockito.mock(KinesisProducer.class);
    UserRecordResult mockResult = Mockito.mock(UserRecordResult.class);
    ListenableFutureTask<UserRecordResult> future = ListenableFutureTask.create(() -> {
      return mockResult;
    });
    Mockito.doThrow(new IllegalArgumentException()).when(mock).addUserRecord(anyString(), anyString(), anyObject());
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ServiceCase.execute(standalone, msg);
      fail();
    } catch (ServiceException expected) {

    }
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
