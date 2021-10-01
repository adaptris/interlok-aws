package com.adaptris.aws2.s3;

import com.adaptris.aws2.s3.acl.S3ObjectAcl;
import com.adaptris.aws2.s3.acl.S3ObjectAclGrant;
import com.adaptris.aws2.s3.acl.S3ObjectAclGranteeCanonicalUser;
import com.adaptris.aws2.s3.acl.S3ObjectAclPermission;
import com.adaptris.aws2.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.lms.FileBackedMessageFactory;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.interlok.cloud.RemoteBlobFilterWrapper;
import com.adaptris.util.KeyValuePair;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;

@SuppressWarnings("deprecation")
public class MockedOperationTest {

  @Test
  public void testCopy_NoDestinationBucket() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    CopyObjectResponse result = CopyObjectResponse.builder().build();
    Mockito.when(client.copyObject((CopyObjectRequest)any())).thenReturn(result);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    CopyOperation op =
        new CopyOperation().withDestinationObjectName("destKey")
            .withObjectName("key")
            .withBucket("bucketName");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }

  @Test
  public void testCopy() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    CopyObjectResponse result = CopyObjectResponse.builder().build();
    Mockito.when(client.copyObject((CopyObjectRequest)any())).thenReturn(result);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    CopyOperation op = new CopyOperation().withDestinationBucket("destBucket")
        .withDestinationObjectName("destKey").withObjectName("key").withBucket("bucketName");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }

  @Test
  public void testExtendedCopy() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);

    GetObjectTaggingResponse mockTagResult = Mockito.mock(GetObjectTaggingResponse.class);
    List<Tag> mockTags = new ArrayList<>(Arrays.asList(Tag.builder().key("hello").value("world").build()));
    Mockito.when(mockTagResult.tagSet()).thenReturn(mockTags);
    Mockito.when(client.getObjectTagging((GetObjectTaggingRequest)any())).thenReturn(mockTagResult);

    CopyObjectResponse result = CopyObjectResponse.builder().build();
    Mockito.when(client.copyObject((CopyObjectRequest)any())).thenReturn(result);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();

    ExtendedCopyOperation op =
        new ExtendedCopyOperation().withObjectMetadata(new S3ServerSideEncryption())
            .withObjectTags(new KeyValuePair("goodbye", "cruel world"))
            .withDestinationBucket("destBucket")
        .withDestinationObjectName("destKey").withObjectName("key").withBucket("bucketName");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }

  @Test
  public void testDelete() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.doAnswer((i)-> null).when(client).deleteObject(DeleteObjectRequest.builder().bucket(anyString()).key(anyString()).build());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    DeleteOperation op = new DeleteOperation()
        .withObjectName("key")
        .withBucket("bucketName");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }


  @Test
  public void testGet() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    S3Object result = Mockito.mock(S3Object.class);

    ResponseInputStream resultStream = new ResponseInputStream(result, AbortableInputStream.create(new ByteArrayInputStream("Hello World".getBytes())));

    Mockito.when(client.getObject((GetObjectRequest)anyObject())).thenReturn(resultStream);
    Mockito.when(resultStream.response()).thenReturn(result);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3GetOperation op = new S3GetOperation()
        .withObjectName("srcKey")
        .withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }

  @Test
  public void testTag_WithFilter() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.doAnswer((i) -> {
      return null;
    }).when(client).putObjectTagging((PutObjectTaggingRequest) anyObject());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader("hello", "world");

    TagOperation tag = new TagOperation()
        .withTagMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(tag, wrapper, msg);
  }

  @Test
  public void testTag_NoFilter() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.doAnswer((i)-> {return null;}).when(client).putObjectTagging((PutObjectTaggingRequest) anyObject());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader("hello", "world");

    TagOperation tag = new TagOperation()
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(tag, wrapper, msg);
  }

  @Test
  public void testDownloadOperation_WithTempDir() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Download downloadObject = Mockito.mock(Download.class);

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(downloadObject.isDone()).thenReturn(false, false, true);
    Mockito.doAnswer((i)-> {return null;}).when(downloadObject).waitForCompletion();

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();

    DownloadOperation downloader = new DownloadOperation()
        .withTempDirectory(new File(System.getProperty("java.io.tmpdir")).getCanonicalPath())
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(downloader, wrapper, msg);
  }

  @Test
  public void testDownloadOperation_NoTempDir() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Download downloadObject = Mockito.mock(Download.class);

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(downloadObject.isDone()).thenReturn(false, false, true);
    Mockito.doAnswer((i)-> {return null;}).when(downloadObject).waitForCompletion();

    AdaptrisMessage msg = new FileBackedMessageFactory().newMessage();

    DownloadOperation downloader = new DownloadOperation()
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(downloader, wrapper, msg);
  }

  @Test
  public void testUpload() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Upload uploadObject = Mockito.mock(Upload.class);

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");
    UploadOperation uploader = new UploadOperation()
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testUpload_WithMetadata() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Upload uploadObject = Mockito.mock(Upload.class);

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello");

    UploadOperation uploader = new UploadOperation()
        .withObjectMetadata(new S3ServerSideEncryption())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testUpload_withCannedAcl() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Upload uploadObject = Mockito.mock(Upload.class);

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");
    UploadOperation uploader = new UploadOperation()
      .withCannedObjectAcl(S3ObjectCannedAcl.BUCKET_OWNER_FULL_CONTROL.name())
      .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testUpload_withAcl() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Upload uploadObject = Mockito.mock(Upload.class);

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    Mockito.when(client.getS3AccountOwner()).thenReturn(new Owner("234", "alias"));

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");
    UploadOperation uploader = new UploadOperation()
      .withObjectAcl(
        new S3ObjectAcl(Arrays.asList(
          new S3ObjectAclGrant(new S3ObjectAclGranteeCanonicalUser("123"), S3ObjectAclPermission.READ),
          new S3ObjectAclGrant(new S3ObjectAclGranteeCanonicalUser(), S3ObjectAclPermission.READ)
          )
        )
      )
      .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testCheckFileExists() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    Mockito.when(client.doesObjectExist(anyString(), anyString())).thenReturn(false).thenReturn(true);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello");
    CheckFileExistsOperation checker = new CheckFileExistsOperation()
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    try {
      execute(checker, wrapper, msg);
      fail();
    }
    catch (Exception expected) {

    }
    execute(checker, wrapper, msg);
  }

  @Test
  public void testGetTagOperation() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    GetObjectTaggingResponse result = Mockito.mock(GetObjectTaggingResponse.class);
    List<Tag> tags = new ArrayList<Tag>(Arrays.asList(Tag.builder().key("hello").value("world").build()));
    Mockito.when(result.tagSet()).thenReturn(tags);

    Mockito.when(client.getObjectTagging((GetObjectTaggingRequest)anyObject())).thenReturn(result);
    GetTagOperation getTags = new GetTagOperation().withTagMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(getTags, wrapper, msg);
    assertTrue(msg.headersContainsKey("hello"));
    assertEquals("world", msg.getMetadataValue("hello"));
  }

  @Test
  public void testListOperationNoFilter() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ListObjectsV2Response result = Mockito.mock(ListObjectsV2Response.class);
    S3Object sbase = createSummary("srcBucket", "srcKeyPrefix/");
    S3Object s1 = createSummary("srcBucket", "srcKeyPrefix/file.json");
    S3Object s2 = createSummary("srcBucket", "srcKeyPrefix/file2.csv");
    List<S3Object> list = new ArrayList<>(Arrays.asList(sbase, s1, s2));
    Mockito.when(result.contents()).thenReturn(list);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    ListOperation ls = new ListOperation()
        .withPrefix("srcKeyPrefix/")
        .withBucket("srcBucket");
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(ls, wrapper, msg);
    assertEquals(
        "srcKeyPrefix/" + System.lineSeparator() +
        "srcKeyPrefix/file.json" + System.lineSeparator() +
        "srcKeyPrefix/file2.csv" + System.lineSeparator(), msg.getContent());
  }


  @Test
  public void testListOperation_RemoteBlobFilter() throws Exception {
    S3Client client = Mockito.mock(S3Client.class);
    ListObjectsV2Response result = Mockito.mock(ListObjectsV2Response.class);
    S3Object sbase = createSummary("srcBucket", "srcKeyPrefix/");
    S3Object s1 = createSummary("srcBucket", "srcKeyPrefix/file.json");
    S3Object s2 = createSummary("srcBucket", "srcKeyPrefix/file2.csv");

    List<S3Object> list = new ArrayList<>(Arrays.asList(sbase, s1, s2));
    Mockito.when(result.contents()).thenReturn(list);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    RemoteBlobFilterWrapper filter =
        new RemoteBlobFilterWrapper().withFilterExpression(".*\\.json").withFilterImp(RegexFileFilter.class.getCanonicalName());
    ListOperation ls = new ListOperation().withFilter(filter)
        .withPrefix("srcKeyPrefix/")
        .withBucket("srcBucket");

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(ls, wrapper, msg);
    assertEquals("srcKeyPrefix/file.json" + System.lineSeparator(), msg.getContent());
  }

  private void execute(S3Operation op, ClientWrapper wrapper, AdaptrisMessage msg)
      throws Exception {
    op.prepare();
    op.execute(wrapper, msg);
  }

  public static S3Object createSummary(String bucket, String key) {
    S3Object.Builder builder = S3Object.builder();
    //sbase.setBucketName(bucket);
    builder.key(key);
    builder.size(0L);
    builder.lastModified(Instant.now());
    return builder.build();
  }

}
