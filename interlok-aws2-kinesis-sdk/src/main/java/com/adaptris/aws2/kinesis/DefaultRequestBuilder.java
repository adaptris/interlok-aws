package com.adaptris.aws2.kinesis;

import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Collections;

/**
 * The default request builder.
 * <p>
 * This simply takes the entire {@link AdaptrisMessage} payload and treats it as a single
 * {@code PutRecordsRequestEntry}.
 * </p>
 *
 * @config aws2-kinesis-default-request-builder
 * @since 4.3.0
 */
@XStreamAlias("aws2-kinesis-default-request-builder")
public class DefaultRequestBuilder implements RequestBuilder {

  @Override
  public Iterable<PutRecordsRequestEntry> build(String partitionKey, AdaptrisMessage message) {
    PutRecordsRequestEntry.Builder builder = PutRecordsRequestEntry.builder();
    builder.data(SdkBytes.fromByteArray(message.getPayload()));
    builder.partitionKey(partitionKey);
    return Collections.singletonList(builder.build());
  }
}
