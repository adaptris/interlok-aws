package com.adaptris.aws.s3.retry;

import java.util.Iterator;
import java.util.function.Function;
import com.adaptris.interlok.cloud.RemoteBlob;

class RetryableBlobIterable implements Iterable<RemoteBlob>, Iterator<RemoteBlob> {

  private transient Iterator<RemoteBlob> wrappedIterator;
  private transient Iterable<RemoteBlob> wrappedIterable;
  private transient Function<String, String> nameMapper;

  public RetryableBlobIterable(Iterable<RemoteBlob> itr, Function<String, String> msgIdFunction) {
    wrappedIterable = itr;
    nameMapper = msgIdFunction;
  }

  @Override
  public Iterator<RemoteBlob> iterator() {
    if (wrappedIterator != null) {
      throw new IllegalStateException("iterator already invoked");
    }
    wrappedIterator = wrappedIterable.iterator();
    return this;
  }

  @Override
  public boolean hasNext() {
    return wrappedIterator.hasNext();
  }

  @Override
  public RemoteBlob next() {
    RemoteBlob blob = wrappedIterator.next();
    // Leave the bucket as null so that it is not rendered (or rendered as null.
    return new RemoteBlob.Builder()
        .setLastModified(blob.getLastModified()).setName(nameMapper.apply(blob.getName()))
        .setSize(blob.getSize()).build();
  }

}
