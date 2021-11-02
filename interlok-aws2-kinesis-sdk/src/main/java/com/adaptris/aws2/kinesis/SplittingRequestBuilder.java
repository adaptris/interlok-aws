package com.adaptris.aws2.kinesis;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.services.splitter.MessageSplitter;
import com.adaptris.interlok.util.CloseableIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Iterator;

/**
 * Request Builder implementation that allows you to split the message into a number of
 * {@code PutRecordsRequestEntry} records.
 *
 * @config aws2-kinesis-splitting-request-builder
 * @since 4.3.0
 */
@XStreamAlias("aws2-kinesis-splitting-request-builder")
public class SplittingRequestBuilder implements RequestBuilder {

  /**
   * The message splitter implementation that splits the message into appropriate kinesis records.
   *
   */
  @Getter
  @Setter
  @Valid
  @NonNull
  @NotNull
  private MessageSplitter messageSplitter;

  public SplittingRequestBuilder withMessageSplitter(MessageSplitter s) {
    setMessageSplitter(s);
    return this;
  }

  @Override
  public Iterable<PutRecordsRequestEntry> build(String partitionKey, AdaptrisMessage message) throws CoreException {
    return new PutRecordsRequestEntryIterator(message, partitionKey, getMessageSplitter());
  }

  private static class PutRecordsRequestEntryIterator implements Iterator<PutRecordsRequestEntry>, CloseableIterable<PutRecordsRequestEntry> {

    private final String partitionKey;
    private final CloseableIterable<AdaptrisMessage> messages;
    private final  Iterator<AdaptrisMessage> messageIterator;

    private boolean iteratorInvoked = false;

    PutRecordsRequestEntryIterator(AdaptrisMessage message, String partitionKey, MessageSplitter messageSplitter) throws CoreException {
      this.partitionKey = partitionKey;
      messages = CloseableIterable.ensureCloseable(messageSplitter.splitMessage(message));
      messageIterator = messages.iterator();
    }

    @Override
    public boolean hasNext() {
      return messageIterator.hasNext();
    }

    @Override
    public PutRecordsRequestEntry next() {
      AdaptrisMessage message = messageIterator.next();
      PutRecordsRequestEntry.Builder builder = PutRecordsRequestEntry.builder();
      builder.data(SdkBytes.fromByteArray(message.getPayload()));
      builder.partitionKey(partitionKey);
      return builder.build();
    }

    @Override
    public void close() throws IOException {
      messages.close();
    }

    @Override
    public Iterator<PutRecordsRequestEntry> iterator() {
      if (iteratorInvoked) {
        throw new IllegalStateException("iterator already invoked");
      }
      iteratorInvoked = true;
      return this;
    }
  }
}
