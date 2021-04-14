package com.adaptris.aws.kinesis;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.services.splitter.MessageSplitter;
import com.adaptris.interlok.util.CloseableIterable;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

@XStreamAlias("aws-kinesis-splitting-request-builder")
public class SplittingRequestBuilder implements RequestBuilder {

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
      this.messages = CloseableIterable.ensureCloseable(messageSplitter.splitMessage(message));
      this.messageIterator = messages.iterator();
    }

    @Override
    public boolean hasNext() {
      return messageIterator.hasNext();
    }

    @Override
    public PutRecordsRequestEntry next() {
      AdaptrisMessage message = messageIterator.next();
      PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
      putRecordsRequestEntry.setData(ByteBuffer.wrap(message.getPayload()));
      putRecordsRequestEntry.setPartitionKey(partitionKey);
      return putRecordsRequestEntry;
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
