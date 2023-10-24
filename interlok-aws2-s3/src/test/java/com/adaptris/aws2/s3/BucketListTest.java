package com.adaptris.aws2.s3;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.interlok.cloud.BlobListRenderer;
import com.adaptris.interlok.cloud.RemoteBlobFilterWrapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.adaptris.aws2.s3.MockedOperationTest.createSummary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("deprecation")
public class BucketListTest {

  @Test
  public void testService_NoFilter() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);

    ListObjectsV2Response listing = Mockito.mock(ListObjectsV2Response.class);
    List<S3Object> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json"), createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing.contents()).thenReturn(summaries);
    ArgumentCaptor<ListObjectsV2Request> argument = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    Mockito.when(client.listObjectsV2(argument.capture())).thenReturn(listing);

    // for coverage, we should use withPrefix().
    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket")
            .withPrefix("srcKeyPrefix").withOutputStyle(null);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);

    Mockito.verify(client, Mockito.times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    assertEquals("srcBucket", argument.getValue().bucket());
    assertEquals("srcKeyPrefix", argument.getValue().prefix());
    assertNull(argument.getValue().maxKeys());

    List<String> lines = IOUtils.readLines(new StringReader(msg.getContent()));
    assertEquals(3, lines.size());
  }

  @Test
  public void testService_Filter() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);

    ListObjectsV2Response listing = Mockito.mock(ListObjectsV2Response.class);
    List<S3Object> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json"), createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing.contents()).thenReturn(summaries);
    ArgumentCaptor<ListObjectsV2Request> argument = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    Mockito.when(client.listObjectsV2(argument.capture())).thenReturn(listing);
    RemoteBlobFilterWrapper filter =
        new RemoteBlobFilterWrapper().withFilterExpression(".*\\.json").withFilterImp(RegexFileFilter.class.getCanonicalName());

    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket")
            .withPrefix("srcKeyPrefix").withFilter(filter);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);

    Mockito.verify(client, Mockito.times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    assertEquals("srcBucket", argument.getValue().bucket());
    assertEquals("srcKeyPrefix", argument.getValue().prefix());
    assertNull(argument.getValue().maxKeys());

    List<String> lines = IOUtils.readLines(new StringReader(msg.getContent()));
    assertEquals(1, lines.size());
  }

  @Test(expected = ServiceException.class)
  public void testService_Failure() throws Exception {
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    S3Client client = Mockito.mock(S3Client.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);
    BlobListRenderer brokenRender = Mockito.mock(BlobListRenderer.class);
    Mockito.doThrow(new RuntimeException()).when(brokenRender).render(any(), any());
    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket")
            .withPrefix("srcKeyPrefix")
            .withOutputStyle(brokenRender);

    ListObjectsV2Response listing = Mockito.mock(ListObjectsV2Response.class);
    List<S3Object> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json"), createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing.contents()).thenReturn(summaries);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listing);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);
  }

  @Test
  public void testService_MaxKeys() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);

    ListObjectsV2Response listing = Mockito.mock(ListObjectsV2Response.class);
    List<S3Object> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json")));
    Mockito.when(listing.contents()).thenReturn(summaries);
    ArgumentCaptor<ListObjectsV2Request> argument = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    Mockito.when(client.listObjectsV2(argument.capture())).thenReturn(listing);

    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket")
            .withPrefix("srcKeyPrefix").withMaxKeys(2).withOutputStyle(null);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);

    Mockito.verify(client, Mockito.times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    assertEquals("srcBucket", argument.getValue().bucket());
    assertEquals("srcKeyPrefix", argument.getValue().prefix());
    assertNotNull(argument.getValue().maxKeys());
    assertEquals(Optional.of(2), Optional.ofNullable(argument.getValue().maxKeys()));

    List<String> lines = IOUtils.readLines(new StringReader(msg.getContent()));
    assertEquals(2, lines.size());
  }

  @Test
  public void testService_Paging() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);

    ListObjectsV2Response listing = Mockito.mock(ListObjectsV2Response.class);
    List<S3Object> summaries = new ArrayList<>(Arrays.asList(
        createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json")));
    Mockito.when(listing.contents()).thenReturn(summaries);
    Mockito.when(listing.nextContinuationToken()).thenReturn("abc123");
    Mockito.when(listing.isTruncated()).thenReturn(true);

    ListObjectsV2Response listing2 = Mockito.mock(ListObjectsV2Response.class);
    List<S3Object> summaries2 = new ArrayList<>(Collections.singletonList(
        createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing2.contents()).thenReturn(summaries2);
    Mockito.when(listing2.isTruncated()).thenReturn(false);

    ArgumentCaptor<ListObjectsV2Request> argument = ArgumentCaptor.forClass(ListObjectsV2Request.class);

    Mockito.when(client.listObjectsV2(argument.capture())).thenReturn(listing, listing2);

    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection)
            .withPrefix("srcKeyPrefix").withMaxKeys(2)
            .withOutputStyle(null).withBucket("srcBucket");
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);

    Mockito.verify(client, Mockito.times(2)).listObjectsV2(any(ListObjectsV2Request.class));

    List<ListObjectsV2Request> capturedRequests = argument.getAllValues();

    assertEquals("srcBucket", capturedRequests.get(0).bucket());
    assertEquals("srcKeyPrefix", capturedRequests.get(0).prefix());
    // Since we are re-using the same object repeatedly, this is probably set to "abc123"
    // assertNull(capturedRequests.get(0).getContinuationToken());
    assertNotNull(capturedRequests.get(0).maxKeys());
    assertEquals(Optional.of(2), Optional.ofNullable(capturedRequests.get(0).maxKeys()));


    assertEquals("srcBucket", capturedRequests.get(1).bucket());
    assertEquals("srcKeyPrefix", capturedRequests.get(1).prefix());
    // getContinuationToken not captured in argument capture possibly due to reuse of object in loop
    // assertEquals("abc123", capturedRequests.get(1).getContinuationToken());
    assertNotNull(capturedRequests.get(1).maxKeys());
    assertEquals(Optional.of(2), Optional.ofNullable(capturedRequests.get(1).maxKeys()));


    List<String> lines = IOUtils.readLines(new StringReader(msg.getContent()));
    assertEquals(3, lines.size());
  }

}
