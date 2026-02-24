package com.adaptris.aws2.s3.retry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.adaptris.aws2.s3.AmazonS3Connection;
import com.adaptris.aws2.s3.ClientWrapper;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.junit.scaffolding.BaseCase;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3RetryStoreTest extends BaseCase {


  private static final String CLASS_UNDER_TEST_KEY = "ClassUnderTest";

  @Test
  public void testReport() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    ListObjectsV2Response result = Mockito.mock(ListObjectsV2Response.class);
    String msgId1 = UUID.randomUUID().toString();
    String msgId2 = UUID.randomUUID().toString();

    List<S3Object> list = new ArrayList<>(
        Arrays.asList(createSummary("bucket", "MyPrefix/" + msgId1 + "/payload.blob"),
            createSummary("bucket", "MyPrefix/" + msgId1 + "/metadata.properties"),
            createSummary("bucket", "MyPrefix/" + msgId2 + "/payload.blob"),
            createSummary("bucket", "MyPrefix/" + msgId2 + "/metadata.properties")));

    Mockito.when(result.contents()).thenReturn(list);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);

    try {
      start(store);
      Iterable<RemoteBlob> blobs = store.report(false);
      List<String> blobNames = StreamSupport.stream(blobs.spliterator(), false)
          .map(RemoteBlob::getName).toList();
      assertTrue(blobNames.stream().anyMatch(name -> name.contains(msgId1)));
      assertTrue(blobNames.stream().anyMatch(name -> name.contains(msgId2)));
    } finally {
      stop(store);
    }
  }

    @Test
    public void testReportWithErrorMessage_Success() throws Exception {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      String msgId = UUID.randomUUID().toString();
      List<S3Object> list = Arrays.asList(
          createSummary("bucket", "MyPrefix/" + msgId + "/payload.blob"),
          createSummary("bucket", "MyPrefix/" + msgId + "/metadata.properties")
      );
      ListObjectsV2Response result = Mockito.mock(ListObjectsV2Response.class);
      Mockito.when(result.contents()).thenReturn(list);
      Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

      // Mock stacktrace.txt content
      String stacktraceContent = "Simulated error message\nSecond line of stacktrace";
      GetObjectResponse mockResponse = Mockito.mock(GetObjectResponse.class);
      ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
          mockResponse,
          AbortableInputStream.create(new ByteArrayInputStream(stacktraceContent.getBytes(StandardCharsets.UTF_8))));
      Mockito.when(client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

      S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);
        Iterable<RemoteBlob> blobs = store.report(true);
        for (RemoteBlob blob : blobs) {
          // The blob name should be just the msgId
          assertEquals(msgId, blob.getName());
          // The error summary should contain the first line of the stacktrace
          assertEquals("Simulated error message", blob.getErrorSummary());
        }
      } finally {
        stop(store);
      }
    }

    @Test
    public void testReportWithErrorMessage_Exception() throws Exception {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      String msgId = UUID.randomUUID().toString();
      List<S3Object> list = Arrays.asList(
          createSummary("bucket", "MyPrefix/" + msgId + "/payload.blob"),
          createSummary("bucket", "MyPrefix/" + msgId + "/metadata.properties")
      );
      ListObjectsV2Response result = Mockito.mock(ListObjectsV2Response.class);
      Mockito.when(result.contents()).thenReturn(list);
      Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
      Mockito.when(client.getObject(any(GetObjectRequest.class))).thenThrow(new RuntimeException("Simulated error"));

      S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);
        Iterable<RemoteBlob> blobs = store.report(true);
        for (RemoteBlob blob : blobs) {
          // When stacktrace retrieval fails, the blob name should just be the msgId
          assertEquals(msgId, blob.getName());
          // And errorSummary should be null since we couldn't retrieve it
          assertNull(blob.getErrorSummary());
        }
      } finally {
        stop(store);
      }
    }


  // Designed to check to toMessageId method and other things that are predicated on getPrefix
  // it's all about the coverage...
  @Test
  public void testPrefix() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store_no_prefix = new S3RetryStore().withBucket("bucket").withConnection(conn);
    S3RetryStore store_with_prefix =
        new S3RetryStore().withBucket("bucket").withPrefix("prefix").withConnection(conn);
    try {
      start(store_no_prefix, store_with_prefix);
      assertEquals("prefix/msgId", store_no_prefix.toMessageID("prefix/msgId/payload.blob"));
      assertEquals("msgId", store_with_prefix.toMessageID("prefix/msgId/payload.blob"));

      assertEquals("msgId/payload.blob", store_no_prefix.buildObjectName("msgId", "payload.blob"));
      assertEquals("prefix/msgId/payload.blob",
          store_with_prefix.buildObjectName("msgId", "payload.blob"));
    } finally {
      stop(store_no_prefix, store_with_prefix);
    }
  }

  @Test
  public void testWrite_Exception() throws Exception {

    Assertions.assertThrows(InterlokException.class, () -> {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      Mockito.when(client.putObject((PutObjectRequest)any(), (RequestBody)any())).thenThrow(new RuntimeException());

      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
      msg.addMessageHeader("hello", "world");

      S3RetryStore store =
          new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);
        store.write(msg);
      } finally {
        stop(store);
      }
    });
    
  }

  @Test
  public void testDelete() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    Mockito.when(client.deleteObject((DeleteObjectRequest)any())).thenReturn(DeleteObjectResponse.builder().build());

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      store.delete("XXXX");
    } finally {
      stop(store);
    }
  }


  @Test
  public void testGetMetadata() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    S3Object mS3Object = Mockito.mock(S3Object.class);
    ByteArrayInputStream mStream = createMetadataStream();
    ResponseInputStream resultStream = new ResponseInputStream(mS3Object, AbortableInputStream.create(mStream));

    long size = mStream.available();
    Mockito.when(mS3Object.size()).thenReturn(size);
    Mockito.when(client.getObject((GetObjectRequest) any())).thenReturn(resultStream);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      Map<String, String> map = store.getMetadata("XXXX");
      assertTrue(map.containsKey(CLASS_UNDER_TEST_KEY));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testGetMetadata_Exception() throws Exception {
    Assertions.assertThrows(InterlokException.class, () -> {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      Mockito.when(client.getObject((GetObjectRequest) any()))
          .thenThrow(new RuntimeException());

      AmazonS3Connection conn = buildConnection(wrapper);

      S3RetryStore store =
          new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);
        Map<String, String> map = store.getMetadata("XXXX");
        assertTrue(map.containsKey(CLASS_UNDER_TEST_KEY));
      } finally {
        stop(store);
      }
    });
  }

  @Test
  public void testBuildForRetry() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    S3Object pS3Object = Mockito.mock(S3Object.class);
    ByteArrayInputStream pStream = new ByteArrayInputStream("hello world".getBytes(StandardCharsets.UTF_8));
    ResponseInputStream resultStream = new ResponseInputStream(pS3Object, AbortableInputStream.create(pStream));

    long size = pStream.available();
    Mockito.when(pS3Object.size()).thenReturn(size);
    Mockito.when(client.getObject((GetObjectRequest) any())).thenReturn(resultStream);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      Map<String, String> metadata = new HashMap<>();
      metadata.put(CLASS_UNDER_TEST_KEY, S3RetryStore.class.getCanonicalName());
      String expectedGuid = UUID.randomUUID().toString();
      AdaptrisMessage msg = store.buildForRetry(expectedGuid, metadata, null);
      assertEquals(expectedGuid, msg.getUniqueId());
      assertTrue(msg.headersContainsKey(CLASS_UNDER_TEST_KEY));
      assertEquals(S3RetryStore.class.getCanonicalName(),
          msg.getMetadataValue(CLASS_UNDER_TEST_KEY));
      Mockito.verify(client, Mockito.times(3)).deleteObject((DeleteObjectRequest) any());
    } finally {
      stop(store);
    }
  }

  @Test
  public void testBuildForRetry_Failure() throws Exception {
    Assertions.assertThrows(InterlokException.class, () -> {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      Mockito.when(client.getObject((GetObjectRequest) any())).thenThrow(new RuntimeException());

      AmazonS3Connection conn = buildConnection(wrapper);

      S3RetryStore store =
          new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);
        Map<String, String> metadata = new HashMap<>();
        AdaptrisMessage msg = store.buildForRetry("XXX", metadata, null);
      } finally {
        stop(store);
      }
    });
  }

  @Test
  public void testGetStackTrace_Success() throws Exception {
    final String STACKTRACE_CONTENT = "stacktrace content";
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock the response object
    GetObjectResponse mockResponse = Mockito.mock(GetObjectResponse.class);

    ResponseInputStream<GetObjectResponse> responseStream =
      new ResponseInputStream<>(mockResponse, AbortableInputStream.create(
        new ByteArrayInputStream(STACKTRACE_CONTENT.getBytes(StandardCharsets.UTF_8))));

    Mockito.when(client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withConnection(conn);
    try {
      start(store);
      String stackTrace = store.getStackTrace("messageId");
      assertEquals(STACKTRACE_CONTENT, stackTrace);
    } finally {
      stop(store);
    }
  }

  @Test
  public void testGetStackTrace_Exception() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    Mockito.when(client.getObject(any(GetObjectRequest.class))).thenThrow(new RuntimeException());

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withConnection(conn);
    try {
      start(store);
      assertThrows(InterlokException.class, () -> {
          store.getStackTrace("messageId");
      }, "Exception should be wrapped in InterlokException");
    } finally {
      stop(store);
    }
  }

  @Test
  public void testMetadataAsMessage() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withConnection(conn);
    try {
      start(store);

      // Create a message with metadata
      AdaptrisMessage originalMsg = AdaptrisMessageFactory.getDefaultInstance().newMessage("test payload");
      originalMsg.addMessageHeader("key1", "value1");
      originalMsg.addMessageHeader("key2", "value2");
      originalMsg.addMessageHeader(CLASS_UNDER_TEST_KEY, "testValue");

      // Use reflection to access the private method
      java.lang.reflect.Method method = S3RetryStore.class.getDeclaredMethod("metadataAsMessage", AdaptrisMessage.class);
      method.setAccessible(true);
      AdaptrisMessage metadataMsg = (AdaptrisMessage) method.invoke(store, originalMsg);

      // Verify the metadata message contains properties
      assertNotNull(metadataMsg);
      String content = metadataMsg.getContent();
      assertTrue(content.contains("key1"));
      assertTrue(content.contains("value1"));
      assertTrue(content.contains("key2"));
      assertTrue(content.contains("value2"));
      assertTrue(content.contains(CLASS_UNDER_TEST_KEY));
      assertTrue(content.contains("testValue"));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadMetadata() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock successful upload
    Mockito.when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(null);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);

      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("test");
      msg.setUniqueId("test-message-id");
      msg.addMessageHeader("testKey", "testValue");

      // Use reflection to call private uploadMetadata method
      java.lang.reflect.Method method = S3RetryStore.class.getDeclaredMethod("uploadMetadata", AdaptrisMessage.class);
      method.setAccessible(true);
      method.invoke(store, msg);

      // Verify putObject was called
      Mockito.verify(client, Mockito.atLeastOnce()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadMetadata_Exception() throws Exception {
    Assertions.assertThrows(Exception.class, () -> {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      // Mock upload failure
      Mockito.when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenThrow(new RuntimeException("Upload failed"));

      S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);

        AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("test");
        msg.setUniqueId("test-message-id");
        msg.addMessageHeader("testKey", "testValue");

        // Use reflection to call private uploadMetadata method
        java.lang.reflect.Method method = S3RetryStore.class.getDeclaredMethod("uploadMetadata", AdaptrisMessage.class);
        method.setAccessible(true);
        method.invoke(store, msg);
      } finally {
        stop(store);
      }
    });
  }

  @Test
  public void testUploadStacktrace_WithStacktrace() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock successful upload
    Mockito.when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(null);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);

      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("test");
      msg.setUniqueId("test-message-id");

      // Add an exception to the message to create a stacktrace
      Exception testException = new Exception("Test exception");
      msg.addObjectHeader(com.adaptris.core.CoreConstants.OBJ_METADATA_EXCEPTION, testException);

      // Use reflection to call private uploadStacktrace method
      java.lang.reflect.Method method = S3RetryStore.class.getDeclaredMethod("uploadStacktrace", AdaptrisMessage.class);
      method.setAccessible(true);
      method.invoke(store, msg);

      // Verify putObject was called (stacktrace was uploaded)
      Mockito.verify(client, Mockito.atLeastOnce()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadStacktrace_NoStacktrace() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
    Mockito.when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);

      // Create message without exception/stacktrace
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("test");
      msg.setUniqueId("test-message-id");

      // Use reflection to call private uploadStacktrace method
      java.lang.reflect.Method method = S3RetryStore.class.getDeclaredMethod("uploadStacktrace", AdaptrisMessage.class);
      method.setAccessible(true);
      method.invoke(store, msg);

      // Verify putObject was NOT called (no stacktrace to upload)
      Mockito.verify(client, Mockito.never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadStacktrace_Exception() throws Exception {
    Assertions.assertThrows(Exception.class, () -> {
      S3Client client = Mockito.mock(S3Client.class);
      ClientWrapper wrapper = Mockito.mock(ClientWrapper.class);
      Mockito.when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      // Mock upload failure
      Mockito.when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
          .thenThrow(new RuntimeException("Upload failed"));

      S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
      try {
        start(store);

        AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("test");
        msg.setUniqueId("test-message-id");

        // Add an exception to the message
        Exception testException = new Exception("Test exception");
        msg.addObjectHeader(com.adaptris.core.CoreConstants.OBJ_METADATA_EXCEPTION, testException);

        // Use reflection to call private uploadStacktrace method
        java.lang.reflect.Method method = S3RetryStore.class.getDeclaredMethod("uploadStacktrace", AdaptrisMessage.class);
        method.setAccessible(true);
        method.invoke(store, msg);
      } finally {
        stop(store);
      }
    });
  }

  private AmazonS3Connection buildConnection(ClientWrapper wrapper) {
    AmazonS3Connection connection = Mockito.mock(AmazonS3Connection.class);
    Mockito.when(connection.retrieveConnection(ClientWrapper.class)).thenReturn(wrapper);
    return connection;
  }

  public static S3Object createSummary(String bucket, String key) {
    S3Object.Builder builder = S3Object.builder();
    builder.key(key);
    builder.size(0L);
    builder.lastModified(Instant.now());
//    S3ObjectSummary sbase = new S3ObjectSummary();
//    sbase.setBucketName(bucket);
    return builder.build();
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
