package com.adaptris.aws.sns;

import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.amazonaws.services.sns.AmazonSNSClient;

public abstract class NotificationProducer extends ProduceOnlyProducerImp {

  @Override
  public void init() throws CoreException {
  }

  @Override
  public void start() throws CoreException {
    // By default, and static lifecycle is handled by the connection
  }

  @Override
  public void stop() {
    // By default, and static lifecycle is handled by the connection
  }

  @Override
  public void close() {
    // By default, and static lifecycle is handled by the connection
  }

  @Override
  public void prepare() throws CoreException {
    // By default, and static lifecycle is handled by the connection
  }

  protected AmazonSNSClient client() {
    return retrieveConnection(AmazonSNSConnection.class).amazonClient();
  }

}
