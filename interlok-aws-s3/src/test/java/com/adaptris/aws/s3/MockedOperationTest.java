package com.adaptris.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import com.adaptris.aws.s3.acl.S3ObjectAcl;
import com.adaptris.aws.s3.acl.S3ObjectAclGrant;
import com.adaptris.aws.s3.acl.S3ObjectAclGranteeCanonicalUser;
import com.adaptris.aws.s3.acl.S3ObjectAclPermission;
import com.adaptris.aws.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.lms.FileBackedMessageFactory;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.interlok.cloud.RemoteBlobFilterWrapper;
import com.adaptris.util.KeyValuePair;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;

@SuppressWarnings("deprecation")
public class MockedOperationTest {

  @Test
  public void testCopy_NoDestinationBucket() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    CopyObjectResult result = new CopyObjectResult();
    Mockito.when(client.copyObject(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(result);
    Mockito.when(client.copyObject(any())).thenReturn(result);
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
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    CopyObjectResult result = new CopyObjectResult();
    Mockito.when(client.copyObject(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(result);
    Mockito.when(client.copyObject(any())).thenReturn(result);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    CopyOperation op = new CopyOperation().withDestinationBucket("destBucket")
        .withDestinationObjectName("destKey").withObjectName("key").withBucket("bucketName");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }

  @Test
  public void testExtendedCopy() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);

    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentType("text/plain");
    Mockito.when(client.getObjectMetadata(anyString(), anyString())).thenReturn(objectMetadata);

    GetObjectTaggingResult mockTagResult = Mockito.mock(GetObjectTaggingResult.class);
    List<Tag> mockTags = new ArrayList<Tag>(Arrays.asList(new Tag("hello", "world")));
    Mockito.when(mockTagResult.getTagSet()).thenReturn(mockTags);
    Mockito.when(client.getObjectTagging(any())).thenReturn(mockTagResult);


    CopyObjectResult result = new CopyObjectResult();
    Mockito.when(client.copyObject(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(result);
    Mockito.when(client.copyObject(any())).thenReturn(result);
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
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    Mockito.doAnswer((i)-> {return null;}).when(client).deleteObject(anyString(), anyString());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    DeleteOperation op = new DeleteOperation()
        .withObjectName("key")
        .withBucket("bucketName");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }


  @Test
  public void testGet() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    S3Object result = Mockito.mock(S3Object.class);
    ObjectMetadata metadata = Mockito.mock(ObjectMetadata.class);
    S3ObjectInputStream resultStream = new S3ObjectInputStream(new ByteArrayInputStream("Hello World".getBytes()), null);
    Mockito.when(metadata.getContentLength()).thenReturn(100L);
    Mockito.when(result.getObjectMetadata()).thenReturn(metadata);
    Mockito.when(result.getObjectContent()).thenReturn(resultStream);
    Mockito.when(client.getObject(any(GetObjectRequest.class))).thenReturn(result);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3GetOperation op = new S3GetOperation()
        .withObjectName("srcKey")
        .withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(op, wrapper, msg);
  }

  @Test
  public void testTag_WithFilter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    Mockito.doAnswer((i) -> {
      return null;
    }).when(client).setObjectTagging(any(SetObjectTaggingRequest.class));
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
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    Mockito.doAnswer((i)-> {return null;}).when(client).setObjectTagging(any(SetObjectTaggingRequest.class));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader("hello", "world");

    TagOperation tag = new TagOperation()
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client);
    execute(tag, wrapper, msg);
  }

  @Test
  public void testDownloadOperation_WithTempDir() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Download downloadObject = Mockito.mock(Download.class);
    ObjectMetadata metadata = Mockito.mock(ObjectMetadata.class);
    TransferProgress progress = new TransferProgress();

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(downloadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(downloadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i)-> {return null;}).when(downloadObject).waitForCompletion();

    Mockito.when(transferManager.download(any(GetObjectRequest.class), any(File.class))).thenReturn(downloadObject);
    Mockito.when(downloadObject.getObjectMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getUserMetadata()).thenReturn(userMetadata);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();

    DownloadOperation downloader = new DownloadOperation()
        .withTempDirectory(new File(System.getProperty("java.io.tmpdir")).getCanonicalPath())
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(downloader, wrapper, msg);
  }

  @Test
  public void testDownloadOperation_NoTempDir() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Download downloadObject = Mockito.mock(Download.class);
    ObjectMetadata metadata = Mockito.mock(ObjectMetadata.class);
    TransferProgress progress = new TransferProgress();

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(downloadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(downloadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i)-> {return null;}).when(downloadObject).waitForCompletion();

    Mockito.when(transferManager.download(any(GetObjectRequest.class), any(File.class))).thenReturn(downloadObject);
    Mockito.when(downloadObject.getObjectMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getUserMetadata()).thenReturn(userMetadata);

    AdaptrisMessage msg = new FileBackedMessageFactory().newMessage();

    DownloadOperation downloader = new DownloadOperation()
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(downloader, wrapper, msg);
  }

  @Test
  public void testUpload() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Upload uploadObject = Mockito.mock(Upload.class);
    TransferProgress progress = new TransferProgress();

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(uploadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    Mockito.when(transferManager.upload(any(PutObjectRequest.class))).thenReturn(uploadObject);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");
    UploadOperation uploader = new UploadOperation()
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testUpload_WithMetadata() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Upload uploadObject = Mockito.mock(Upload.class);
    TransferProgress progress = new TransferProgress();

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(uploadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    Mockito.when(transferManager.upload(any(PutObjectRequest.class))).thenReturn(uploadObject);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello");

    UploadOperation uploader = new UploadOperation()
        .withObjectMetadata(new S3ServerSideEncryption())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testUpload_withCannedAcl() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Upload uploadObject = Mockito.mock(Upload.class);
    TransferProgress progress = new TransferProgress();

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(uploadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    Mockito.when(transferManager.upload(any(PutObjectRequest.class))).thenReturn(uploadObject);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");
    UploadOperation uploader = new UploadOperation()
      .withCannedObjectAcl(S3ObjectCannedAcl.BUCKET_OWNER_FULL_CONTROL.name())
      .withUserMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testUpload_withAcl() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Upload uploadObject = Mockito.mock(Upload.class);
    TransferProgress progress = new TransferProgress();

    Map<String,String> userMetadata = new HashMap<>();
    userMetadata.put("hello", "world");
    Mockito.when(uploadObject.isDone()).thenReturn(false, false, true);
    Mockito.when(uploadObject.getProgress()).thenReturn(progress);
    Mockito.doAnswer((i)-> {return null;}).when(uploadObject).waitForCompletion();

    Mockito.when(transferManager.upload(any(PutObjectRequest.class))).thenReturn(uploadObject);

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
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(uploader, wrapper, msg);
  }

  @Test
  public void testCheckFileExists() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    Mockito.when(client.doesObjectExist(anyString(), anyString())).thenReturn(false).thenReturn(true);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello");
    CheckFileExistsOperation checker = new CheckFileExistsOperation()
        .withObjectName("srcKey").withBucket("srcBucket");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    try {
      execute(checker, wrapper, msg);
      fail();
    }
    catch (Exception expcted) {

    }
    execute(checker, wrapper, msg);
  }

  @Test
  public void testGetTagOperation() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    GetObjectTaggingResult result = Mockito.mock(GetObjectTaggingResult.class);
    List<Tag> tags = new ArrayList<Tag>(Arrays.asList(new Tag("hello", "world")));
    Mockito.when(result.getTagSet()).thenReturn(tags);

    Mockito.when(client.getObjectTagging(any())).thenReturn(result);
    GetTagOperation getTags = new GetTagOperation().withTagMetadataFilter(new NoOpMetadataFilter())
        .withObjectName("srcKey").withBucket("srcBucket");

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(getTags, wrapper, msg);
    assertTrue(msg.headersContainsKey("hello"));
    assertEquals("world", msg.getMetadataValue("hello"));
  }

  @Test
  public void testListOperationNoFilter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ListObjectsV2Result result = Mockito.mock(ListObjectsV2Result.class);
    S3ObjectSummary sbase = createSummary("srcBucket", "srcKeyPrefix/");
    S3ObjectSummary s1 = createSummary("srcBucket", "srcKeyPrefix/file.json");
    S3ObjectSummary s2 = createSummary("srcBucket", "srcKeyPrefix/file2.csv");
    List<S3ObjectSummary> list = new ArrayList<>(Arrays.asList(sbase, s1, s2));
    Mockito.when(result.getObjectSummaries()).thenReturn(list);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    ListOperation ls = new ListOperation()
        .withPrefix("srcKeyPrefix/")
        .withBucket("srcBucket");
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(ls, wrapper, msg);
    assertEquals(
        "srcKeyPrefix/" + System.lineSeparator() +
        "srcKeyPrefix/file.json" + System.lineSeparator() +
        "srcKeyPrefix/file2.csv" + System.lineSeparator(), msg.getContent());
  }


  @Test
  public void testListOperation_RemoteBlobFilter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    TransferManager transferManager = Mockito.mock(TransferManager.class);
    ListObjectsV2Result result = Mockito.mock(ListObjectsV2Result.class);
    S3ObjectSummary sbase = createSummary("srcBucket", "srcKeyPrefix/");
    S3ObjectSummary s1 = createSummary("srcBucket", "srcKeyPrefix/file.json");
    S3ObjectSummary s2 = createSummary("srcBucket", "srcKeyPrefix/file2.csv");

    List<S3ObjectSummary> list = new ArrayList<>(Arrays.asList(sbase, s1, s2));
    Mockito.when(result.getObjectSummaries()).thenReturn(list);
    Mockito.when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    RemoteBlobFilterWrapper filter =
        new RemoteBlobFilterWrapper().withFilterExpression(".*\\.json").withFilterImp(RegexFileFilter.class.getCanonicalName());
    ListOperation ls = new ListOperation().withFilter(filter)
        .withPrefix("srcKeyPrefix/")
        .withBucket("srcBucket");

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("");
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);
    execute(ls, wrapper, msg);
    assertEquals("srcKeyPrefix/file.json" + System.lineSeparator(), msg.getContent());
  }

  private void execute(S3Operation op, ClientWrapper wrapper, AdaptrisMessage msg)
      throws Exception {
    op.prepare();
    op.execute(wrapper, msg);
  }

  public static S3ObjectSummary createSummary(String bucket, String key) {
    S3ObjectSummary sbase = new S3ObjectSummary();
    sbase.setBucketName(bucket);
    sbase.setKey(key);
    sbase.setSize(0);
    sbase.setLastModified(new Date());
    return sbase;
  }

}
