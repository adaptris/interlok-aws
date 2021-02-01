package com.adaptris.aws.kinesis;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.junit.Test;
import org.mockito.Mockito;

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
  @SuppressWarnings("deprecation")
  public void testProduce() throws Exception {
    KinesisSDKStreamProducer producer =
        new KinesisSDKStreamProducer().withPartitionKey("myPartitionKey");
    producer.setDestination(new ConfiguredProduceDestination("myStreamName"));
    AmazonKinesis mock = Mockito.mock(AmazonKinesis.class);
    PutRecordResult mockResult = Mockito.mock(PutRecordResult.class);
    Mockito.when(mock.putRecord(any())).thenReturn(mockResult);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(standalone, msg);
  }

  @Test
  public void testProduce_NoDestination() throws Exception {
    KinesisSDKStreamProducer producer = new KinesisSDKStreamProducer().withStream("myStreamName").withPartitionKey("myPartitionKey");
    AmazonKinesis mock = Mockito.mock(AmazonKinesis.class);
    PutRecordResult mockResult = Mockito.mock(PutRecordResult.class);
    Mockito.when(mock.putRecord(any())).thenReturn(mockResult);
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ExampleServiceCase.execute(standalone, msg);
  }

  @Test
  public void testProduce_Exception() throws Exception {
    KinesisSDKStreamProducer producer =
        new KinesisSDKStreamProducer().withStream("%message{does not exist}").withPartitionKey("%message{does not exist}");
    AmazonKinesis mock = Mockito.mock(AmazonKinesis.class);
    Mockito.doThrow(new ResourceNotFoundException("Error [does not exist] does not exist")).when(mock).putRecord(any());
    StandaloneProducer standalone = new StandaloneProducer(new MyConnection(mock), producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try {
      ExampleServiceCase.execute(standalone, msg);
      fail();
    } catch (ServiceException expected) {

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
    public AmazonKinesis kinesisClient() throws Exception {
      return producer;
    }

  }
}
