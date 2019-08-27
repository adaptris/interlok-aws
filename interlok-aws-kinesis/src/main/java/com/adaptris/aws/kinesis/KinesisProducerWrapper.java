package com.adaptris.aws.kinesis;

import com.amazonaws.services.kinesis.producer.KinesisProducer;

@FunctionalInterface
public interface KinesisProducerWrapper {

  KinesisProducer kinesisProducer() throws Exception;
}
