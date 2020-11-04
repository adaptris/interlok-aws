package com.adaptris.aws.s3;

import static com.adaptris.aws.s3.LocalstackConfig.MY_TAG;
import static com.adaptris.aws.s3.LocalstackConfig.MY_TAG_TEXT;
import static com.adaptris.aws.s3.LocalstackConfig.S3_BUCKETNAME;
import static com.adaptris.aws.s3.LocalstackConfig.S3_COPY_TO_FILENAME;
import static com.adaptris.aws.s3.LocalstackConfig.S3_FILTER_REGEXP;
import static com.adaptris.aws.s3.LocalstackConfig.S3_UPLOAD_FILENAME;
import static com.adaptris.aws.s3.LocalstackConfig.areTestsEnabled;
import static com.adaptris.aws.s3.LocalstackConfig.build;
import static com.adaptris.aws.s3.LocalstackConfig.getConfiguration;
import static com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RegexMetadataFilter;
import com.adaptris.interlok.cloud.RemoteBlobFilterWrapper;

// A new local stack instance; we're going upload, copy, tag, download, get, delete in that order
// So, sadly we are being really lame, and forcing the ordering...
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackServiceTest {

  private static final String MSG_CONTENTS = "hello world";

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(areTestsEnabled());
  }

  @Test
  public void test_01_CreateBucket() throws Exception {
    CreateBucketOperation create =
        new CreateBucketOperation().withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(create);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS));
  }

  @Test
  public void test_02_Upload() throws Exception {
    UploadOperation upload =
        new UploadOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(upload);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS));
  }

  @Test
  public void test_03_Download() throws Exception {
    DownloadOperation download =
        new DownloadOperation()
        .withObjectName(getConfig(S3_UPLOAD_FILENAME))
        .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(download);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    execute(service, msg);
    assertEquals(MSG_CONTENTS, msg.getContent());
  }

  @Test
  public void test_04_Get() throws Exception {
    S3GetOperation download =
        new S3GetOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));

    S3Service service = build(download);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    execute(service, msg);
    assertEquals(MSG_CONTENTS, msg.getContent());

  }

  @Test
  public void test_05_Copy_WithDestinationBucket() throws Exception {
    CopyOperation copy =
        new CopyOperation().withDestinationBucket(getConfig(S3_BUCKETNAME))
            .withDestinationObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service copyService = build(copy);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    execute(copyService, msg);

    S3GetOperation download =
        new S3GetOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service downloadService = build(download);
    execute(downloadService, msg);
    assertEquals(MSG_CONTENTS, msg.getContent());

  }

  @Test
  public void test_06_Tag() throws Exception {
    tagObject(getConfig(S3_COPY_TO_FILENAME));

    AdaptrisMessage msgWithTags = getTags(getConfig(S3_COPY_TO_FILENAME));
    assertTrue(msgWithTags.headersContainsKey(MY_TAG));
    assertEquals(MY_TAG_TEXT, msgWithTags.getMetadataValue(MY_TAG));
  }

  @Test
  public void test_07_DeleteCopy() throws Exception {
    DeleteOperation delete =
        new DeleteOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service s1 = build(delete);
    execute(s1, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }


  @Test
  public void test_09_List() throws Exception {
    // if it doesn't exist, then it
    // will still pass this test...
    String regexp = getConfiguration().getProperty(S3_FILTER_REGEXP, ".*");
    RemoteBlobFilterWrapper filter =
        new RemoteBlobFilterWrapper().withFilterExpression(regexp).withFilterImp(RegexFileFilter.class.getCanonicalName());

    ListOperation ls =
        new ListOperation().withFilter(filter)
            .withPrefix(null).withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(ls);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    execute(service, msg);
    assertEquals(getConfig(S3_UPLOAD_FILENAME) + System.lineSeparator(), msg.getContent());
  }


  @Test
  public void test_10_ExtendedCopy_WithoutDestinationBucket() throws Exception {
    tagObject(getConfig(S3_UPLOAD_FILENAME));

    ExtendedCopyOperation copy = new ExtendedCopyOperation()
        .withDestinationObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service copyService = build(copy);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    execute(copyService, msg);

    S3GetOperation download = new S3GetOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
        .withBucket(getConfig(S3_BUCKETNAME));
    S3Service downloadService = build(download);
    execute(downloadService, msg);
    assertEquals(MSG_CONTENTS, msg.getContent());

    // Check the tags were preserved
    AdaptrisMessage msgWithTags = getTags(getConfig(S3_COPY_TO_FILENAME));

    assertTrue(msgWithTags.headersContainsKey(MY_TAG));
    assertEquals(MY_TAG_TEXT, msgWithTags.getMetadataValue(MY_TAG));
  }

  @Test
  public void test_11_DeleteCopy() throws Exception {
    DeleteOperation delete =
        new DeleteOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(delete);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());

    CheckFileExistsOperation check =
        new CheckFileExistsOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service checker = build(check);
    try {
      execute(checker, AdaptrisMessageFactory.getDefaultInstance().newMessage());
      fail();
    } catch (ServiceException expected) {

    }
  }

  @Test
  public void test_14_DeleteUploaded() throws Exception {
    DeleteOperation delete =
        new DeleteOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(delete);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());

    CheckFileExistsOperation check =
        new CheckFileExistsOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service checker = build(check);
    try {
      execute(checker, AdaptrisMessageFactory.getDefaultInstance().newMessage());
      fail();
    } catch (ServiceException expected) {

    }
  }


  @Test
  public void test_99_DeleteBucket() throws Exception {
    DeleteBucketOperation delete =
        new DeleteBucketOperation().withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(delete);
    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }

  protected String getConfig(String cfgKey) {
    return getConfiguration().getProperty(cfgKey);
  }

  private void tagObject(String key) throws Exception {
    TagOperation tag = new TagOperation()
        .withTagMetadataFilter(new RegexMetadataFilter().withIncludePatterns(MY_TAG))
        .withObjectName(key).withBucket(getConfig(S3_BUCKETNAME));
    S3Service tagger = build(tag);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(MY_TAG, MY_TAG_TEXT);
    execute(tagger, msg);
  }

  private AdaptrisMessage getTags(String key) throws Exception {
    GetTagOperation getTags = new GetTagOperation().withTagMetadataFilter(new NoOpMetadataFilter())
        .withObjectName(key).withBucket(getConfig(S3_BUCKETNAME));
    S3Service retrieveTags = build(getTags);
    AdaptrisMessage msgWithTags = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    execute(retrieveTags, msgWithTags);
    return msgWithTags;
  }

}
