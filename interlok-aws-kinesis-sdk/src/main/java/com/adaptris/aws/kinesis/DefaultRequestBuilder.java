package com.adaptris.aws.kinesis;

import java.nio.ByteBuffer;
import java.util.Collections;
import com.adaptris.core.AdaptrisMessage;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * The default request builder.
 * <p>
 * This simply takes the entire {@link AdaptrisMessage} payload and treats it as a single
 * {@code PutRecordsRequestEntry}.
 * </p>
 * 
 * @config aws-kinesis-default-request-builder
 */
@XStreamAlias("aws-kinesis-default-request-builder")
public class DefaultRequestBuilder implements RequestBuilder {

  @Override
  public Iterable<PutRecordsRequestEntry> build(String partitionKey, AdaptrisMessage message) {
    PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
    putRecordsRequestEntry.setData(ByteBuffer.wrap(message.getPayload()));
    putRecordsRequestEntry.setPartitionKey(partitionKey);
    return Collections.singletonList(putRecordsRequestEntry);
  }
}
