package com.adaptris.aws.s3.retry;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import com.adaptris.interlok.cloud.RemoteBlob;

public class RetryableBlobIterableTest {

  @Test
  public void testIterator() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(1), (s) -> nameMapper(s));
    Iterator<RemoteBlob> i = itr.iterator();
    assertTrue(i.hasNext());
    RemoteBlob blob = i.next();
    assertFalse(blob.getName().startsWith("my-prefix/"));

  }
  @Test
  public void testIterator_Double() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(10), (s) -> nameMapper(s));
    assertThrows(IllegalStateException.class, ()->{
      itr.iterator();
      itr.iterator();
    }, "Iterator run twice, illegalstate exception has been thrown");  
  }

  @Test
  public void testIterator_WithErrorSummary() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(1), (s) -> nameMapper(s),
        (s) -> "Error: " + s);
    Iterator<RemoteBlob> i = itr.iterator();
    assertTrue(i.hasNext());
    RemoteBlob blob = i.next();
    assertFalse(blob.getName().startsWith("my-prefix/"));
    assertEquals("Error: " + blob.getName(), blob.getErrorSummary());
  }

  @Test
  public void testIterator_WithErrorSummaryReturningNull() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(1), (s) -> nameMapper(s),
        (s) -> null);
    Iterator<RemoteBlob> i = itr.iterator();
    assertTrue(i.hasNext());
    RemoteBlob blob = i.next();
    assertFalse(blob.getName().startsWith("my-prefix/"));
    assertNull(blob.getErrorSummary());
  }

  @Test
  public void testIterator_WithNullErrorSummaryFunction() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(1), (s) -> nameMapper(s));
    Iterator<RemoteBlob> i = itr.iterator();
    assertTrue(i.hasNext());
    RemoteBlob blob = i.next();
    assertFalse(blob.getName().startsWith("my-prefix/"));
    assertNull(blob.getErrorSummary());
  }

  @Test
  public void testIterator_MultipleBlobs_WithErrorSummary() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(10), (s) -> nameMapper(s),
        (s) -> "Error: " + s);
    Iterator<RemoteBlob> i = itr.iterator();
    int count = 0;
    while (i.hasNext()) {
      RemoteBlob blob = i.next();
      assertFalse(blob.getName().startsWith("my-prefix/"));
      assertEquals("Error: " + blob.getName(), blob.getErrorSummary());
      count++;
    }
    assertEquals(10, count);
  }

  private Iterable<RemoteBlob> build(int count) {
    List<RemoteBlob> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(
          new RemoteBlob.Builder().setBucket("bucket").setLastModified(System.currentTimeMillis())
              .setSize(-1).setName("my-prefix/" + UUID.randomUUID().toString()).build());
    }
    return result;
  }

  private String nameMapper(String s) {
    return s.replace("my-prefix/", "");
  }
}
