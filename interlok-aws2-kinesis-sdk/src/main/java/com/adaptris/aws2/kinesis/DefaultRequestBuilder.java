package com.adaptris.aws2.kinesis;

import java.nio.ByteBuffer;
import java.util.Collections;
import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

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
    PutRecordsRequestEntry putRecordsRequestEntry  = PutRecordsRequestEntry.builder()
      .data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(message.getPayload())))
      .partitionKey(partitionKey)
      .build();
    return Collections.singletonList(putRecordsRequestEntry);
  }
}
