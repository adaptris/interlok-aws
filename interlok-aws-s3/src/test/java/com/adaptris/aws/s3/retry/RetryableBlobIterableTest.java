package com.adaptris.aws.s3.retry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
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
  @Test(expected = IllegalStateException.class)
  public void testIterator_Double() throws Exception {
    RetryableBlobIterable itr = new RetryableBlobIterable(build(10), (s) -> nameMapper(s));
    itr.iterator();
    itr.iterator();
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
