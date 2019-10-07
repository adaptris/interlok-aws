package com.adaptris.aws.s3;

import static com.adaptris.aws.s3.MockedOperationTest.createSummary;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.interlok.cloud.BlobListRenderer;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class BucketListTest {

  @Test
  public void testService_NoFilter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);

    ObjectListing listing = Mockito.mock(ObjectListing.class);
    List<S3ObjectSummary> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json"), createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing.getObjectSummaries()).thenReturn(summaries);
    Mockito.when(client.listObjects(anyString(), anyString())).thenReturn(listing);

    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket").withKey("srcKeyPrefix").withOutputStyle(null);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);
    List<String> lines = IOUtils.readLines(new StringReader(msg.getContent()));
    assertEquals(3, lines.size());
  }

  @Test
  public void testService_Filter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);

    ObjectListing listing = Mockito.mock(ObjectListing.class);
    List<S3ObjectSummary> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json"), createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing.getObjectSummaries()).thenReturn(summaries);
    Mockito.when(client.listObjects(anyString(), anyString())).thenReturn(listing);

    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket").withKey("srcKeyPrefix").withFilterSuffix(".json");
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);
    List<String> lines = IOUtils.readLines(new StringReader(msg.getContent()));
    assertEquals(1, lines.size());
  }

  @Test(expected = ServiceException.class)
  public void testService_Failure() throws Exception {
    AmazonS3Connection mockConnection = Mockito.mock(AmazonS3Connection.class);
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    Mockito.when(mockConnection.retrieveConnection(any())).thenReturn(wrapper);
    BlobListRenderer brokenRender = Mockito.mock(BlobListRenderer.class);
    Mockito.doThrow(new RuntimeException()).when(brokenRender).render(any(), any());
    S3BucketList bucket =
        new S3BucketList().withConnection(mockConnection).withBucket("srcBucket").withKey("srcKeyPrefix")
            .withOutputStyle(brokenRender);

    ObjectListing listing = Mockito.mock(ObjectListing.class);
    List<S3ObjectSummary> summaries = new ArrayList<>(Arrays.asList(createSummary("srcBucket", "srcKeyPrefix/"),
        createSummary("srcBucket", "srcKeyPrefix/file.json"), createSummary("srcBucket", "srcKeyPrefix/file2.csv")));
    Mockito.when(listing.getObjectSummaries()).thenReturn(summaries);
    Mockito.when(client.listObjects(anyString(), anyString())).thenReturn(listing);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(bucket, msg);
  }

}
