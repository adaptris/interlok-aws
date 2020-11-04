package com.adaptris.aws.s3.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.aws.s3.AmazonS3Connection;
import com.adaptris.aws.s3.ClientWrapper;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;

public class S3RetryStoreTest {


  private static final String CLASS_UNDER_TEST_KEY = "ClassUnderTest";

  @Test
  public void testReport() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    AmazonS3Connection conn = buildConnection(wrapper);

    ListObjectsV2Result result = Mockito.mock(ListObjectsV2Result.class);
    String msgId1 = UUID.randomUUID().toString();
    String msgId2 = UUID.randomUUID().toString();
    S3ObjectSummary sbase = createSummary("bucket", msgId1 + "/payload.blob");
    S3ObjectSummary s1 = createSummary("bucket", msgId1 + "/metadata.properties");
    S3ObjectSummary s2 = createSummary("bucket", msgId2 + "/payload.blob");
    S3ObjectSummary s3 = createSummary("bucket", msgId2 + "/metadata.properties");

    List<S3ObjectSummary> list = new ArrayList<>(
        Arrays.asList(createSummary("bucket", "MyPrefix/" + msgId1 + "/payload.blob"),
            createSummary("bucket", "MyPrefix/" + msgId1 + "/metadata.properties"),
            createSummary("bucket", "MyPrefix/" + msgId2 + "/payload.blob"),
            createSummary("bucket", "MyPrefix/" + msgId2 + "/metadata.properties")));

    Mockito.when(result.getObjectSummaries()).thenReturn(list);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      Iterable<RemoteBlob> blobs = store.report();
      List<String> blobNames = StreamSupport.stream(blobs.spliterator(), false)
          .map((blob) -> blob.getName()).collect(Collectors.toList());
      assertTrue(blobNames.contains(msgId1));
      assertTrue(blobNames.contains(msgId2));
    } finally {
      BaseCase.stop(store);
    }
  }


  // Designed to check to toMessageId method and other things that are predicated on getPrefix
  // it's all about the coverage...
  @Test
  public void testPrefix() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store_no_prefix = new S3RetryStore().withBucket("bucket").withConnection(conn);
    S3RetryStore store_with_prefix =
        new S3RetryStore().withBucket("bucket").withPrefix("prefix").withConnection(conn);
    try {
      BaseCase.start(store_no_prefix, store_with_prefix);
      assertEquals("prefix/msgId", store_no_prefix.toMessageID("prefix/msgId/payload.blob"));
      assertEquals("msgId", store_with_prefix.toMessageID("prefix/msgId/payload.blob"));

      assertEquals("msgId/payload.blob", store_no_prefix.buildObjectName("msgId", "payload.blob"));
      assertEquals("prefix/msgId/payload.blob",
          store_with_prefix.buildObjectName("msgId", "payload.blob"));
    } finally {
      BaseCase.stop(store_no_prefix, store_with_prefix);
    }
  }

  @Test
  public void testWrite() throws Exception {
    Upload uploadObject = Mockito.mock(Upload.class);
    TransferProgress progress = new TransferProgress();

    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(uploadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i) -> {
      return null;
    }).when(uploadObject).waitForCompletion();

    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    AmazonS3Connection conn = buildConnection(wrapper);

    Mockito.when(transferManager.upload(anyString(), anyString(), (InputStream) any(),
        (ObjectMetadata) any())).thenReturn(uploadObject);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      store.write(msg);
    } finally {
      BaseCase.stop(store);
    }
  }


  @Test(expected = InterlokException.class)
  public void testWrite_Exception() throws Exception {

    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    AmazonS3Connection conn = buildConnection(wrapper);

    Mockito.when(transferManager.upload(anyString(), anyString(), (InputStream) any(),
        (ObjectMetadata) any())).thenThrow(new RuntimeException());

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      store.write(msg);
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test
  public void testDelete() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);
    Mockito.doAnswer((i) -> {
      return null;
    }).when(client).deleteObject(anyString(), anyString());


    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      store.delete("XXXX");
    } finally {
      BaseCase.stop(store);
    }
  }


  @Test
  public void testGetMetadata() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    S3Object mS3Object = Mockito.mock(S3Object.class);
    ObjectMetadata objMetadata = Mockito.mock(ObjectMetadata.class);
    ByteArrayInputStream mStream = createMetadataStream();
    S3ObjectInputStream resultStream =
        new S3ObjectInputStream(mStream, null);
    long size = mStream.available();
    Mockito.when(objMetadata.getContentLength()).thenReturn(size);
    Mockito.when(mS3Object.getObjectMetadata()).thenReturn(objMetadata);
    Mockito.when(mS3Object.getObjectContent()).thenReturn(resultStream);
    Mockito.when(client.getObject((GetObjectRequest) any())).thenReturn(mS3Object);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      Map<String, String> map = store.getMetadata("XXXX");
      assertTrue(map.containsKey(CLASS_UNDER_TEST_KEY));
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test(expected = InterlokException.class)
  public void testGetMetadata_Exception() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    Mockito.when(client.getObject((GetObjectRequest) any()))
        .thenThrow(new RuntimeException());

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      Map<String, String> map = store.getMetadata("XXXX");
      assertTrue(map.containsKey(CLASS_UNDER_TEST_KEY));
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test
  public void testBuildForRetry() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    S3Object pS3Object = Mockito.mock(S3Object.class);
    ObjectMetadata objMetadata = Mockito.mock(ObjectMetadata.class);
    ByteArrayInputStream pStream =
        new ByteArrayInputStream("hello world".getBytes(StandardCharsets.UTF_8));
    S3ObjectInputStream resultStream = new S3ObjectInputStream(pStream, null);
    long size = pStream.available();
    Mockito.when(objMetadata.getContentLength()).thenReturn(size);
    Mockito.when(pS3Object.getObjectMetadata()).thenReturn(objMetadata);
    Mockito.when(pS3Object.getObjectContent()).thenReturn(resultStream);
    Mockito.when(client.getObject((GetObjectRequest) any())).thenReturn(pS3Object);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      Map<String, String> metadata = new HashMap<>();
      metadata.put(CLASS_UNDER_TEST_KEY, S3RetryStore.class.getCanonicalName());
      String expectedGuid = UUID.randomUUID().toString();
      AdaptrisMessage msg = store.buildForRetry(expectedGuid, metadata, null);
      assertEquals(expectedGuid, msg.getUniqueId());
      assertTrue(msg.headersContainsKey(CLASS_UNDER_TEST_KEY));
      assertEquals(S3RetryStore.class.getCanonicalName(),
          msg.getMetadataValue(CLASS_UNDER_TEST_KEY));
    } finally {
      BaseCase.stop(store);
    }
  }

  @Test(expected = InterlokException.class)
  public void testBuildForRetry_Failure() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);
    Mockito.when(wrapper.transferManager()).thenReturn(transferManager);

    Mockito.when(client.getObject((GetObjectRequest) any())).thenThrow(new RuntimeException());

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      BaseCase.start(store);
      Map<String, String> metadata = new HashMap<>();
      AdaptrisMessage msg = store.buildForRetry("XXX", metadata, null);
    } finally {
      BaseCase.stop(store);
    }
  }

  private AmazonS3Connection buildConnection(ClientWrapper wrapper) {
    AmazonS3Connection connection = Mockito.mock(AmazonS3Connection.class);
    Mockito.when(connection.retrieveConnection(ClientWrapper.class)).thenReturn(wrapper);
    return connection;
  }

  public static S3ObjectSummary createSummary(String bucket, String key) {
    S3ObjectSummary sbase = new S3ObjectSummary();
    sbase.setBucketName(bucket);
    sbase.setKey(key);
    sbase.setSize(0);
    sbase.setLastModified(new Date());
    return sbase;
  }

  private Properties createProperties() {
    Properties p = new Properties();
    p.putAll(System.getenv());
    p.setProperty(CLASS_UNDER_TEST_KEY, S3RetryStore.class.getCanonicalName());
    return p;
  }

  private ByteArrayInputStream createMetadataStream() throws IOException {
    return createInputStream(createProperties());
  }

  private ByteArrayInputStream createInputStream(Properties p) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try (OutputStream o = out) {
      p.store(out, "");
    }
    return new ByteArrayInputStream(out.toByteArray());
  }
}
