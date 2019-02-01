package com.adaptris.aws.s3;

import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.mockito.Mockito;

import com.adaptris.aws.s3.meta.S3ObjectMetadata;
import com.adaptris.aws.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.lms.FileBackedMessageFactory;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;

public class MockedOperationTest {

  @Test
  public void testCopy_NoDestinationBucket() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    CopyObjectResult result = new CopyObjectResult();
    Mockito.when(client.copyObject(anyString(), anyString(), anyString(), anyString())).thenReturn(result);    
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    CopyOperation op = new CopyOperation()
        .withDestinationKey(new ConstantDataInputParameter("destKey"))
        .withBucketName(new ConstantDataInputParameter("bucketName"))
        .withKey(new ConstantDataInputParameter("key"));
    ClientWrapper wrapper = new ClientWrapperImpl(client);    
    op.execute(wrapper, msg);
  }

  @Test
  public void testCopy() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    CopyObjectResult result = new CopyObjectResult();
    Mockito.when(client.copyObject(anyString(), anyString(), anyString(), anyString())).thenReturn(result);    
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    CopyOperation op = new CopyOperation()
        .withDestinationBucketName(new ConstantDataInputParameter("destBucket"))
        .withDestinationKey(new ConstantDataInputParameter("destKey"))
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client);    
    op.execute(wrapper, msg);
  }

  @Test
  public void testDelete() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    Mockito.doAnswer((i)-> {return null;}).when(client).deleteObject(anyString(), anyString());    
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    DeleteOperation op = new DeleteOperation()
        .withBucketName(new ConstantDataInputParameter("bucketName"))
        .withKey(new ConstantDataInputParameter("key"));
    ClientWrapper wrapper = new ClientWrapperImpl(client);    
    op.execute(wrapper, msg);
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
    Mockito.when(client.getObject((GetObjectRequest) anyObject())).thenReturn(result);
     
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3GetOperation op = new S3GetOperation()
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client);    
    op.execute(wrapper, msg);
  }
  
  @Test
  public void testTag_WithFilter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    Mockito.doAnswer((i)-> {return null;}).when(client).setObjectTagging((SetObjectTaggingRequest) anyObject());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader("hello", "world");

    TagOperation tag = new TagOperation()
        .withTagMetadataFilter(new NoOpMetadataFilter())
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client);    
    tag.execute(wrapper, msg);    
  }

  @Test
  public void testTag_NoFilter() throws Exception {
    AmazonS3Client client = Mockito.mock(AmazonS3Client.class);
    Mockito.doAnswer((i)-> {return null;}).when(client).setObjectTagging((SetObjectTaggingRequest) anyObject());
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader("hello", "world");

    TagOperation tag = new TagOperation()
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client);    
    tag.execute(wrapper, msg);    
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
    
    Mockito.when(transferManager.download((GetObjectRequest) anyObject(), (File) anyObject())).thenReturn(downloadObject);
    Mockito.when(downloadObject.getObjectMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getUserMetadata()).thenReturn(userMetadata);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();

    DownloadOperation downloader = new DownloadOperation()
        .withTempDirectory(new File(System.getProperty("java.io.tmpdir")).getCanonicalPath())
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);    
    downloader.execute(wrapper, msg);
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
    
    Mockito.when(transferManager.download((GetObjectRequest) anyObject(), (File) anyObject())).thenReturn(downloadObject);
    Mockito.when(downloadObject.getObjectMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getUserMetadata()).thenReturn(userMetadata);

    AdaptrisMessage msg = new FileBackedMessageFactory().newMessage();

    DownloadOperation downloader = new DownloadOperation()
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);    
    downloader.execute(wrapper, msg);
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
    
    Mockito.when(transferManager.upload(anyString(), anyString(), (InputStream) anyObject(), (ObjectMetadata) anyObject())).thenReturn(uploadObject);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello", "UTF-8");
    msg.addMessageHeader("hello", "world");
    UploadOperation uploader = new UploadOperation()
        .withUserMetadataFilter(new NoOpMetadataFilter())
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);    
    uploader.execute(wrapper, msg);
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
    
    Mockito.when(transferManager.upload(anyString(), anyString(), (InputStream) anyObject(), (ObjectMetadata) anyObject())).thenReturn(uploadObject);

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello");

    UploadOperation uploader = new UploadOperation()
        .withObjectMetadata(new S3ServerSideEncryption())
        .withBucketName(new ConstantDataInputParameter("srcBucket"))
        .withKey(new ConstantDataInputParameter("srcKey"));
    ClientWrapper wrapper = new ClientWrapperImpl(client, transferManager);    
    uploader.execute(wrapper, msg);
  }
}
