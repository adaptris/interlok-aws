package com.adaptris.aws.s3.retry;

import static com.adaptris.aws.s3.LocalstackConfig.S3_RETRY_BUCKET_NAME;
import static com.adaptris.aws.s3.LocalstackConfig.S3_RETRY_PREFIX;
import static com.adaptris.aws.s3.LocalstackConfig.areTestsEnabled;
import static com.adaptris.aws.s3.LocalstackConfig.build;
import static com.adaptris.aws.s3.LocalstackConfig.createConnection;
import static com.adaptris.aws.s3.LocalstackConfig.getConfiguration;
import static com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase.execute;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Iterator;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.adaptris.aws.s3.CreateBucketOperation;
import com.adaptris.aws.s3.DeleteBucketOperation;
import com.adaptris.aws.s3.S3Service;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.junit.scaffolding.BaseCase;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackRetryStoreTest {

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
  }

  @Test
  public void test_01_CreateBucket() throws Exception {
    CreateBucketOperation create =
        new CreateBucketOperation().withBucket(getConfig(S3_RETRY_BUCKET_NAME));
    S3Service service = build(create);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }

  @Test
  public void test_10_Write() throws Exception {
    S3RetryStore store = new S3RetryStore().withBucket(getConfig(S3_RETRY_BUCKET_NAME))
        .withPrefix(getConfig(S3_RETRY_PREFIX)).withConnection(createConnection());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    try {
      BaseCase.start(store);
      store.write(msg);
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test
  public void test_20_Report() throws Exception {
    S3RetryStore store = new S3RetryStore().withBucket(getConfig(S3_RETRY_BUCKET_NAME))
        .withPrefix(getConfig(S3_RETRY_PREFIX)).withConnection(createConnection());
    try {
      BaseCase.start(store);
      Iterable<RemoteBlob> blobs = store.report();
      Iterator<RemoteBlob> itr = blobs.iterator();
      assertNotNull(itr);
      assertTrue(itr.hasNext());
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test
  public void test_30_BuildForRetry() throws Exception {
    S3RetryStore store = new S3RetryStore().withBucket(getConfig(S3_RETRY_BUCKET_NAME))
        .withPrefix(getConfig(S3_RETRY_PREFIX)).withConnection(createConnection());
    try {
      BaseCase.start(store);
      Iterable<RemoteBlob> blobs = store.report();
      Iterator<RemoteBlob> itr = blobs.iterator();
      assertNotNull(itr);
      assertTrue(itr.hasNext());
      RemoteBlob blob = itr.next();
      AdaptrisMessage msg = store.buildForRetry(blob.getName());
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test
  public void test_90_Delete() throws Exception {
    S3RetryStore store = new S3RetryStore().withBucket(getConfig(S3_RETRY_BUCKET_NAME))
        .withPrefix(getConfig(S3_RETRY_PREFIX)).withConnection(createConnection());
    try {
      BaseCase.start(store);
      Iterable<RemoteBlob> blobs = store.report();
      for (RemoteBlob blob : blobs) {
        store.delete(blob.getName());
      }
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test
  public void test_99_DeleteBucket() throws Exception {
    DeleteBucketOperation delete =
        new DeleteBucketOperation().withBucket(getConfig(S3_RETRY_BUCKET_NAME));
    S3Service service = build(delete);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }


  protected String getConfig(String cfgKey) {
    return getConfiguration().getProperty(cfgKey);
  }

}
