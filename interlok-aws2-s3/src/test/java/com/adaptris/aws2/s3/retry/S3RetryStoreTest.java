package com.adaptris.aws2.s3.retry;

import static com.adaptris.core.util.LifecycleHelper.start;
import static com.adaptris.core.util.LifecycleHelper.stop;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.adaptris.aws2.s3.AmazonS3Connection;
import com.adaptris.aws2.s3.ClientWrapper;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
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
  public void testWrite_PersistsWorkflowIdInMetadataProperties() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);
    AmazonS3Connection conn = buildConnection(wrapper);

    List<PutObjectRequest> capturedRequests = new ArrayList<>();
    List<RequestBody> capturedBodies = new ArrayList<>();
    doAnswer(invocation -> {
      capturedRequests.add(invocation.getArgument(0));
      capturedBodies.add(invocation.getArgument(1));
      return null;
    }).when(client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("payload");
      msg.setUniqueId("workflow-positive");
      msg.addMessageHeader("workflowId", "wf-123");
      msg.addMessageHeader("otherKey", "otherValue");

      store.write(msg);

      Properties persisted = metadataPropertiesFromWrite(capturedRequests, capturedBodies,
          store.buildObjectName(msg.getUniqueId(), "metadata.properties"));
      assertEquals("wf-123", persisted.getProperty("workflowId"));
      assertEquals("otherValue", persisted.getProperty("otherKey"));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testWrite_AllowsMissingWorkflowIdAndDoesNotPersistKey() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);
    AmazonS3Connection conn = buildConnection(wrapper);

    List<PutObjectRequest> capturedRequests = new ArrayList<>();
    List<RequestBody> capturedBodies = new ArrayList<>();
    doAnswer(invocation -> {
      capturedRequests.add(invocation.getArgument(0));
      capturedBodies.add(invocation.getArgument(1));
      return null;
    }).when(client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("payload");
      msg.setUniqueId("workflow-negative");
      msg.addMessageHeader("onlyKey", "onlyValue");

      assertDoesNotThrow(() -> store.write(msg));

      Properties persisted = metadataPropertiesFromWrite(capturedRequests, capturedBodies,
          store.buildObjectName(msg.getUniqueId(), "metadata.properties"));
      assertNull(persisted.getProperty("workflowId"));
      assertEquals("onlyValue", persisted.getProperty("onlyKey"));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testGetMetadata_MissingWorkflowId_LeadsToWorkflowResolutionFailure() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    Properties stored = new Properties();
    stored.setProperty(CLASS_UNDER_TEST_KEY, S3RetryStore.class.getCanonicalName());
    stored.setProperty("someKey", "someValue");
    ResponseInputStream resultStream = new ResponseInputStream(mock(S3Object.class),
        AbortableInputStream.create(createInputStream(stored)));
    when(client.getObject((GetObjectRequest) any())).thenReturn(resultStream);

    AmazonS3Connection conn = buildConnection(wrapper);
    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      Map<String, String> metadata = store.getMetadata("missing-workflow");
      assertNull(metadata.get("workflowId"));
      CoreException ex = assertThrows(CoreException.class, () -> requireWorkflowId(metadata));
      assertEquals("No Workflow [null] found", ex.getMessage());
    } finally {
      stop(store);
    }
  }

  @Test
  public void testGetMetadata_LegacyWorkflowidOnly_NoFallbackToWorkflowId() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    Properties stored = new Properties();
    stored.setProperty("workflowid", "legacy-value");
    stored.setProperty("someKey", "someValue");
    ResponseInputStream resultStream = new ResponseInputStream(mock(S3Object.class),
        AbortableInputStream.create(createInputStream(stored)));
    when(client.getObject((GetObjectRequest) any())).thenReturn(resultStream);

    AmazonS3Connection conn = buildConnection(wrapper);
    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      Map<String, String> metadata = store.getMetadata("legacy-workflowid");
      assertEquals("legacy-value", metadata.get("workflowid"));
      assertNull(metadata.get("workflowId"));
      assertThrows(CoreException.class, () -> requireWorkflowId(metadata));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testReport() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    ListObjectsV2Response result = mock(ListObjectsV2Response.class);
    String msgId1 = UUID.randomUUID().toString();
    String msgId2 = UUID.randomUUID().toString();

    List<S3Object> list = new ArrayList<>(
        Arrays.asList(createSummary("MyPrefix/" + msgId1 + "/payload.blob"),
            createSummary("MyPrefix/" + msgId1 + "/metadata.properties"),
            createSummary("MyPrefix/" + msgId2 + "/payload.blob"),
            createSummary("MyPrefix/" + msgId2 + "/metadata.properties")));

    when(result.contents()).thenReturn(list);
    when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

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
  public void testReportMaxKeys_DefaultsTo1000() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    ListObjectsV2Response result = mock(ListObjectsV2Response.class);
    // Create 1000 payload.blob files with interleaved metadata.properties files (3000 keys total)
    List<S3Object> allObjects = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      String msgId = "msg-" + i;
      allObjects.add(createSummary("MyPrefix/" + msgId + "/payload.blob"));
      allObjects.add(createSummary("MyPrefix/" + msgId + "/metadata.properties"));
      allObjects.add(createSummary("MyPrefix/" + msgId + "/stacktrace.txt"));
    }

    when(result.contents()).thenReturn(allObjects);
    when(result.isTruncated()).thenReturn(false);
    when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);

      assertEquals(1000, store.getReportMaxMessages());
      List<String> blobNames = StreamSupport.stream(store.report(false).spliterator(), false)
          .map(RemoteBlob::getName).toList();

      // Should retrieve exactly 1000 message IDs (not 3000 keys)
      assertEquals(1000, blobNames.size());
    } finally {
      stop(store);
    }
  }

  @Test
  public void testReportMaxKeys_UsesConfiguredValue() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    ListObjectsV2Response result = mock(ListObjectsV2Response.class);
    // Create 17 payload.blob files with interleaved metadata.properties files (51 keys total)
    List<S3Object> allObjects = new ArrayList<>();
    for (int i = 0; i < 17; i++) {
      String msgId = "msg-" + i;
      allObjects.add(createSummary("MyPrefix/" + msgId + "/payload.blob"));
      allObjects.add(createSummary("MyPrefix/" + msgId + "/metadata.properties"));
      allObjects.add(createSummary("MyPrefix/" + msgId + "/stacktrace.txt"));
    }

    when(result.contents()).thenReturn(allObjects);
    when(result.isTruncated()).thenReturn(false);
    when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    store.setReportMaxMessages(17);

    try {
      start(store);

      assertEquals(17, store.getReportMaxMessages());
      List<String> blobNames = StreamSupport.stream(store.report(false).spliterator(), false)
          .map(RemoteBlob::getName).toList();

      // Should retrieve exactly 17 message IDs (not 51 keys)
      assertEquals(17, blobNames.size());
    } finally {
      stop(store);
    }
  }

  @Test
  public void testReportMaxKeys_HandlesMultiplePaginatedResponses() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // S3 ListObjectsV2 returns at most 1000 keys per page. Build 1000 failed messages (3000 keys)
    // and split them into realistic 1000-key pages. This naturally exercises page boundaries where
    // a message's 3 keys may span multiple responses.
    List<S3Object> allObjects = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      String msgId = "msg-" + i;
      allObjects.add(createSummary("MyPrefix/" + msgId + "/payload.blob"));
      allObjects.add(createSummary("MyPrefix/" + msgId + "/metadata.properties"));
      allObjects.add(createSummary("MyPrefix/" + msgId + "/stacktrace.txt"));
    }

    List<S3Object> firstPageObjects = new ArrayList<>(allObjects.subList(0, 1000));
    List<S3Object> secondPageObjects = new ArrayList<>(allObjects.subList(1000, 2000));
    List<S3Object> thirdPageObjects = new ArrayList<>(allObjects.subList(2000, 3000));

    ListObjectsV2Response firstPage = mock(ListObjectsV2Response.class);
    when(firstPage.contents()).thenReturn(firstPageObjects);
    when(firstPage.isTruncated()).thenReturn(true);
    when(firstPage.nextContinuationToken()).thenReturn("continuation-token-1");

    ListObjectsV2Response secondPage = mock(ListObjectsV2Response.class);
    when(secondPage.contents()).thenReturn(secondPageObjects);
    when(secondPage.isTruncated()).thenReturn(true);
    when(secondPage.nextContinuationToken()).thenReturn("continuation-token-2");

    ListObjectsV2Response thirdPage = mock(ListObjectsV2Response.class);
    when(thirdPage.contents()).thenReturn(thirdPageObjects);
    when(thirdPage.isTruncated()).thenReturn(false);

    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(firstPage)
        .thenReturn(secondPage)
        .thenReturn(thirdPage);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);

    try {
      start(store);

      List<String> blobNames = StreamSupport.stream(store.report(false).spliterator(), false)
          .map(RemoteBlob::getName).toList();

      // Should retrieve exactly 1000 message IDs across paginated responses
      assertEquals(1000, blobNames.size());

      // Verify pagination occurred across realistic 1000-key pages.
      verify(client, times(3)).listObjectsV2(any(ListObjectsV2Request.class));
    } finally {
      stop(store);
    }
  }

    @Test
    public void testReportWithErrorMessage_Success() throws Exception {
      S3Client client = mock(S3Client.class);
      ClientWrapper wrapper = mock(ClientWrapper.class);
      when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      String msgId = UUID.randomUUID().toString();
      List<S3Object> list = Arrays.asList(
          createSummary("MyPrefix/" + msgId + "/payload.blob"),
          createSummary("MyPrefix/" + msgId + "/metadata.properties")
      );
      ListObjectsV2Response result = mock(ListObjectsV2Response.class);
      when(result.contents()).thenReturn(list);
      when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

      // Mock stacktrace.txt content
      String stacktraceContent = "Simulated error message\nSecond line of stacktrace";
      GetObjectResponse mockResponse = mock(GetObjectResponse.class);
      ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
          mockResponse,
          AbortableInputStream.create(new ByteArrayInputStream(stacktraceContent.getBytes(StandardCharsets.UTF_8))));
      when(client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

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
      S3Client client = mock(S3Client.class);
      ClientWrapper wrapper = mock(ClientWrapper.class);
      when(wrapper.amazonClient()).thenReturn(client);

      AmazonS3Connection conn = buildConnection(wrapper);

      String msgId = UUID.randomUUID().toString();
      List<S3Object> list = Arrays.asList(
          createSummary("MyPrefix/" + msgId + "/payload.blob"),
          createSummary("MyPrefix/" + msgId + "/metadata.properties")
      );
      ListObjectsV2Response result = mock(ListObjectsV2Response.class);
      when(result.contents()).thenReturn(list);
      when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
      when(client.getObject(any(GetObjectRequest.class))).thenThrow(new RuntimeException("Simulated error"));

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
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore storeNoPrefix = new S3RetryStore().withBucket("bucket").withConnection(conn);
    S3RetryStore storeWithPrefix =
        new S3RetryStore().withBucket("bucket").withPrefix("prefix").withConnection(conn);
    try {
      start(storeNoPrefix, storeWithPrefix);
      assertEquals("prefix/msgId", storeNoPrefix.toMessageID("prefix/msgId/payload.blob"));
      assertEquals("msgId", storeWithPrefix.toMessageID("prefix/msgId/payload.blob"));

      assertEquals("msgId/payload.blob", storeNoPrefix.buildObjectName("msgId", "payload.blob"));
      assertEquals("prefix/msgId/payload.blob",
          storeWithPrefix.buildObjectName("msgId", "payload.blob"));
    } finally {
      stop(storeNoPrefix, storeWithPrefix);
    }
  }

  @Test
  public void testWrite_Exception() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    when(client.putObject((PutObjectRequest)any(), (RequestBody)any())).thenThrow(new RuntimeException());

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      assertThrows(InterlokException.class, () -> store.write(msg));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testDelete() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    when(client.deleteObject((DeleteObjectRequest)any())).thenReturn(DeleteObjectResponse.builder().build());

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
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    S3Object mS3Object = mock(S3Object.class);
    ByteArrayInputStream mStream = createMetadataStream();
    ResponseInputStream resultStream = new ResponseInputStream(mS3Object, AbortableInputStream.create(mStream));

    long size = mStream.available();
    when(mS3Object.size()).thenReturn(size);
    when(client.getObject((GetObjectRequest) any())).thenReturn(resultStream);

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
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    when(client.getObject((GetObjectRequest) any()))
        .thenThrow(new RuntimeException());

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      assertThrows(InterlokException.class, () -> store.getMetadata("XXXX"));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testBuildForRetry() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    S3Object pS3Object = mock(S3Object.class);
    ByteArrayInputStream pStream = new ByteArrayInputStream("hello world".getBytes(StandardCharsets.UTF_8));
    ResponseInputStream resultStream = new ResponseInputStream(pS3Object, AbortableInputStream.create(pStream));

    long size = pStream.available();
    when(pS3Object.size()).thenReturn(size);
    when(client.getObject((GetObjectRequest) any())).thenReturn(resultStream);

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
       verify(client, times(3)).deleteObject((DeleteObjectRequest) any());
    } finally {
      stop(store);
    }
  }

  @Test
  public void testBuildForRetry_Failure() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    when(client.getObject((GetObjectRequest) any())).thenThrow(new RuntimeException());

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store =
        new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      Map<String, String> metadata = new HashMap<>();
      assertThrows(InterlokException.class, () -> store.buildForRetry("XXX", metadata, null));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testGetStackTrace_Success() throws Exception {
    final String STACKTRACE_CONTENT = "stacktrace content";
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock the response object
    GetObjectResponse mockResponse = mock(GetObjectResponse.class);

    ResponseInputStream<GetObjectResponse> responseStream =
      new ResponseInputStream<>(mockResponse, AbortableInputStream.create(
        new ByteArrayInputStream(STACKTRACE_CONTENT.getBytes(StandardCharsets.UTF_8))));

    when(client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

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
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    when(client.getObject(any(GetObjectRequest.class))).thenThrow(new RuntimeException());

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withConnection(conn);
    try {
      start(store);
      assertThrows(InterlokException.class, () -> store.getStackTrace("messageId"),
          "Exception should be wrapped in InterlokException");
    } finally {
      stop(store);
    }
  }

  @Test
  public void testMetadataAsMessage() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

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
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock successful upload
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
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
      verify(client, Mockito.atLeastOnce()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadMetadata_Exception() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock upload failure
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
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
      assertThrows(Exception.class, () -> method.invoke(store, msg));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadStacktrace_WithStacktrace() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock successful upload
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
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
      verify(client, Mockito.atLeastOnce()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadStacktrace_NoStacktrace() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

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
      verify(client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testUploadStacktrace_Exception() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);

    // Mock upload failure
    when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
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
      assertThrows(Exception.class, () -> method.invoke(store, msg));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testReport_ExceptionDuringListObjects() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenThrow(new RuntimeException("Simulated error"));

    AmazonS3Connection conn = buildConnection(wrapper);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withPrefix("MyPrefix").withConnection(conn);
    try {
      start(store);
      assertThrows(InterlokException.class, () -> store.report(false));
    } finally {
      stop(store);
    }
  }

  @Test
  public void testRetryContractNoOpMethods() throws Exception {
    S3Client client = mock(S3Client.class);
    ClientWrapper wrapper = mock(ClientWrapper.class);
    when(wrapper.amazonClient()).thenReturn(client);

    AmazonS3Connection conn = buildConnection(wrapper);
    AmazonS3Connection replacementConn = mock(AmazonS3Connection.class);

    S3RetryStore store = new S3RetryStore().withBucket("bucket").withConnection(conn);
    try {
      start(store);

      assertDoesNotThrow(() -> store.acknowledge("ack-id"));
      assertDoesNotThrow(store::deleteAcknowledged);
      assertNull(store.obtainExpiredMessages());
      assertDoesNotThrow(() -> store.updateRetryCount("message-id"));
      assertNull(store.obtainMessagesToRetry());

      store.makeConnection(replacementConn);
      assertSame(store.getConnection(), conn);
    } finally {
      stop(store);
    }
  }

  private AmazonS3Connection buildConnection(ClientWrapper wrapper) {
    AmazonS3Connection connection = mock(AmazonS3Connection.class);
    when(connection.retrieveConnection(ClientWrapper.class)).thenReturn(wrapper);
    return connection;
  }

  public static S3Object createSummary(String key) {
    S3Object.Builder builder = S3Object.builder();
    builder.key(key);
    builder.size(0L);
    builder.lastModified(Instant.now());
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

  private Properties metadataPropertiesFromWrite(List<PutObjectRequest> requests, List<RequestBody> requestBodies,
      String metadataObjectName) throws Exception {
    for (int i = 0; i < requests.size(); i++) {
      PutObjectRequest req = requests.get(i);
      if (metadataObjectName.equals(req.key())) {
        return asProperties(requestBodies.get(i));
      }
    }
    fail("metadata.properties was not uploaded");
    return new Properties();
  }

  private Properties asProperties(RequestBody body) throws Exception {
    Properties properties = new Properties();
    try (InputStream in = body.contentStreamProvider().newStream()) {
      properties.load(in);
    }
    return properties;
  }

  private String requireWorkflowId(Map<String, String> metadata) throws CoreException {
    String workflowId = metadata.get("workflowId");
    if (workflowId == null) {
      throw new CoreException("No Workflow [null] found");
    }
    return workflowId;
  }
}
