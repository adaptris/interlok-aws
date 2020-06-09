package com.adaptris.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Properties;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.CustomEndpoint;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RegexMetadataFilter;
import com.adaptris.core.util.PropertyHelper;
import com.adaptris.interlok.cloud.RemoteBlobFilterWrapper;

// A new local stack instance; we're going upload, copy, tag, download, get, delete in that order
// So, sadly we are being really lame, and forcing the ordering...
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackServiceTest {

  private static final String MY_TAG_TEXT = "text";
  private static final String MY_TAG = "MyTag";
  private static final String TESTS_ENABLED = "localstack.tests.enabled";
  private static final String S3_SIGNING_REGION = "localstack.s3.signingRegion";
  private static final String S3_URL = "localstack.s3.url";
  private static final String S3_UPLOAD_FILENAME = "localstack.s3.upload.filename";
  private static final String S3_COPY_TO_FILENAME = "localstack.s3.copy.filename";
  private static final String S3_BUCKETNAME = "localstack.s3.bucketname";
  private static final String S3_FILTER_SUFFIX = "localstack.s3.ls.filterSuffix";
  private static final String S3_FILTER_REGEXP = "localstack.s3.ls.filterRegexp";
  private static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

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
    ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS));
  }

  @Test
  public void test_02_Upload() throws Exception {
    UploadOperation upload =
        new UploadOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(upload);
    ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS));
  }

  @Test
  public void test_03_Download() throws Exception {
    DownloadOperation download =
        new DownloadOperation()
        .withObjectName(getConfig(S3_UPLOAD_FILENAME))
        .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(download);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(service, msg);
    assertEquals(MSG_CONTENTS, msg.getContent());
  }

  @Test
  public void test_04_Get() throws Exception {
    S3GetOperation download =
        new S3GetOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));

    S3Service service = build(download);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(service, msg);
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
    ServiceCase.execute(copyService, msg);

    S3GetOperation download =
        new S3GetOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service downloadService = build(download);
    ServiceCase.execute(downloadService, msg);
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
    ServiceCase.execute(s1, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }


  @Test
  @SuppressWarnings("deprecation")
  public void test_08_List_Legacy() throws Exception {
    ListOperation ls = new ListOperation()
        .withFilterSuffix(new ConstantDataInputParameter(getConfig(S3_FILTER_SUFFIX)))
        .withPrefix(null)
        .withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(ls);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(service, msg);
    assertEquals(config.getProperty(S3_UPLOAD_FILENAME) + System.lineSeparator(), msg.getContent());
  }

  @Test
  public void test_09_List() throws Exception {
    String regexp = config.getProperty(S3_FILTER_REGEXP, ".*"); // if it doesn't exist, then it will still pass this test...
    RemoteBlobFilterWrapper filter =
        new RemoteBlobFilterWrapper().withFilterExpression(regexp).withFilterImp(RegexFileFilter.class.getCanonicalName());

    ListOperation ls =
        new ListOperation().withFilter(filter)
            .withPrefix(null).withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(ls);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(service, msg);
    assertEquals(config.getProperty(S3_UPLOAD_FILENAME) + System.lineSeparator(), msg.getContent());
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
    ServiceCase.execute(copyService, msg);

    S3GetOperation download = new S3GetOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
        .withBucket(getConfig(S3_BUCKETNAME));
    S3Service downloadService = build(download);
    ServiceCase.execute(downloadService, msg);
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
    ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());

    CheckFileExistsOperation check =
        new CheckFileExistsOperation().withObjectName(getConfig(S3_COPY_TO_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service checker = build(check);
    try {
      ServiceCase.execute(checker, AdaptrisMessageFactory.getDefaultInstance().newMessage());
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
    ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());

    CheckFileExistsOperation check =
        new CheckFileExistsOperation().withObjectName(getConfig(S3_UPLOAD_FILENAME))
            .withBucket(getConfig(S3_BUCKETNAME));
    S3Service checker = build(check);
    try {
      ServiceCase.execute(checker, AdaptrisMessageFactory.getDefaultInstance().newMessage());
      fail();
    } catch (ServiceException expected) {

    }
  }


  @Test
  public void test_99_DeleteBucket() throws Exception {
    DeleteBucketOperation delete =
        new DeleteBucketOperation().withBucket(getConfig(S3_BUCKETNAME));
    S3Service service = build(delete);
    ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }

  protected static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(config.getProperty(TESTS_ENABLED, "false"));
  }

  protected S3Service build(S3Operation operation) {
    String serviceEndpoint = config.getProperty(S3_URL);
    String signingRegion = config.getProperty(S3_SIGNING_REGION);
    AmazonS3Connection connection = new AmazonS3Connection()
        .withCredentialsProviderBuilder(
            new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("TEST", "TEST")))
        .withCustomEndpoint(new CustomEndpoint().withServiceEndpoint(serviceEndpoint).withSigningRegion(signingRegion));
    return new S3Service(connection, operation);
  }

  protected String getConfig(String cfgKey) {
    return config.getProperty(cfgKey);
  }

  private void tagObject(String key) throws Exception {
    TagOperation tag = new TagOperation()
        .withTagMetadataFilter(new RegexMetadataFilter().withIncludePatterns(MY_TAG))
        .withObjectName(key).withBucket(getConfig(S3_BUCKETNAME));
    S3Service tagger = build(tag);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMessageHeader(MY_TAG, MY_TAG_TEXT);
    ServiceCase.execute(tagger, msg);
  }

  private AdaptrisMessage getTags(String key) throws Exception {
    GetTagOperation getTags = new GetTagOperation().withTagMetadataFilter(new NoOpMetadataFilter())
        .withObjectName(key).withBucket(getConfig(S3_BUCKETNAME));
    S3Service retrieveTags = build(getTags);
    AdaptrisMessage msgWithTags = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    ServiceCase.execute(retrieveTags, msgWithTags);
    return msgWithTags;
  }

}
