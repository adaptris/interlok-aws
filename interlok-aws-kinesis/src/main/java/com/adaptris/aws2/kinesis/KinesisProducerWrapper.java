package com.adaptris.aws2.kinesis;

import com.amazonaws.services.kinesis.producer.KinesisProducer;

@FunctionalInterface
public interface KinesisProducerWrapper {

  KinesisProducer kinesisProducer() throws Exception;
}
