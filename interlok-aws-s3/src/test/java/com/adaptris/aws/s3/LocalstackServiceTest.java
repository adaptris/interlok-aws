package com.adaptris.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Properties;
import org.apache.commons.lang3.BooleanUtils;
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
import com.adaptris.interlok.config.DataInputParameter;

// A new local stack instance; we're going upload, copy, tag, download, get, delete in that order
// So, sadly we are being really lame, and forcing the ordering...
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LocalstackServiceTest {

  private static final String TESTS_ENABLED = "localstack.tests.enabled";
  private static final String S3_SIGNING_REGION = "localstack.s3.signingRegion";
  private static final String S3_URL = "localstack.s3.url";
  private static final String S3_UPLOAD_FILENAME = "localstack.s3.upload.filename";
  private static final String S3_COPY_TO_FILENAME = "localstack.s3.copy.filename";
  private static final String S3_BUCKETNAME = "localstack.s3.bucketname";
  private static final String S3_FILTER_PREFIX = "localstack.s3.ls.filterSuffix";
  private static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  private static Properties config = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  private static final String MSG_CONTENTS = "hello world";

  @Before
  public void setUp() throws Exception {

  }

  @Test
  public void test_01_CreateBucket() throws Exception {
    if (areTestsEnabled()) {
      CreateBucketOperation create = new CreateBucketOperation().withBucketName(getInputParameter(S3_BUCKETNAME));
      S3Service service = build(create);
      ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS));
    } else {
      System.err.println("localstack tests disabled; not executing test_01_CreateBucket");
    }
  }

  @Test
  public void test_02_Upload() throws Exception {
    if (areTestsEnabled()) {
      UploadOperation upload = new UploadOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service service = build(upload);
      ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage(MSG_CONTENTS));
    } else {
      System.err.println("localstack tests disabled; not executing test_02_Upload");
    }
  }

  @Test
  public void test_03_Download() throws Exception {
    if (areTestsEnabled()) {
      DownloadOperation download = new DownloadOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service service = build(download);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      ServiceCase.execute(service, msg);
      assertEquals(MSG_CONTENTS, msg.getContent());
    } else {
      System.err.println("localstack tests disabled; not executing test_03_Download");
    }
  }

  @Test
  public void test_04_Get() throws Exception {
    if (areTestsEnabled()) {
      S3GetOperation download = new S3GetOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service service = build(download);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      ServiceCase.execute(service, msg);
      assertEquals(MSG_CONTENTS, msg.getContent());
    } else {
      System.err.println("localstack tests disabled; not executing test_04_Get");
    }
  }

  @Test
  public void test_05_Copy_WithDestinationBucket() throws Exception {
    if (areTestsEnabled()) {
      CopyOperation copy = new CopyOperation().withDestinationBucketName(getInputParameter(S3_BUCKETNAME))
          .withDestinationKey(getInputParameter(S3_COPY_TO_FILENAME)).withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service copyService = build(copy);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      ServiceCase.execute(copyService, msg);

      S3GetOperation download = new S3GetOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service downloadService = build(download);
      ServiceCase.execute(downloadService, msg);
      assertEquals(MSG_CONTENTS, msg.getContent());
    } else {
      System.err.println("localstack tests disabled; not executing test_05_Copy_WithDestinationBucket");
    }
  }

  @Test
  public void test_06_Tag() throws Exception {
    if (areTestsEnabled()) {
      TagOperation tag = new TagOperation().withTagMetadataFilter(new RegexMetadataFilter().withIncludePatterns("MyTag"))
          .withBucketName(getInputParameter(S3_BUCKETNAME)).withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service tagger = build(tag);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      msg.addMessageHeader("MyTag", "text");
      ServiceCase.execute(tagger, msg);

      GetTagOperation getTags = new GetTagOperation().withTagMetadataFilter(new NoOpMetadataFilter())
          .withBucketName(getInputParameter(S3_BUCKETNAME)).withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service retrieveTags = build(getTags);
      AdaptrisMessage msgWithTags = AdaptrisMessageFactory.getDefaultInstance().newMessage();

      ServiceCase.execute(retrieveTags, msgWithTags);
      assertTrue(msgWithTags.headersContainsKey("MyTag"));
      assertEquals("text", msgWithTags.getMetadataValue("MyTag"));
    } else {
      System.err.println("localstack tests disabled; not executing test_06_Tag");
    }
  }

  @Test
  public void test_07_DeleteCopy() throws Exception {
    if (areTestsEnabled()) {
      DeleteOperation delete = new DeleteOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service s1 = build(delete);
      ServiceCase.execute(s1, AdaptrisMessageFactory.getDefaultInstance().newMessage());
    } else {
      System.err.println("localstack tests disabled; not executing test_07_DeleteCopy");
    }
  }


  @Test
  public void test_08_List() throws Exception {
    if (areTestsEnabled()) {
      ListOperation ls = new ListOperation()
          .withFilterSuffix(getInputParameter(S3_FILTER_PREFIX))
          .withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter("/"));
      S3Service service = build(ls);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      ServiceCase.execute(service, msg);
      assertEquals(config.getProperty(S3_UPLOAD_FILENAME) + System.lineSeparator(), msg.getContent());
    } else {
      System.err.println("localstack tests disabled; not executing test_07_List");
    }
  }

  @Test
  public void test_09_Copy_WithoutDestinationBucket() throws Exception {
    if (areTestsEnabled()) {
      CopyOperation copy = new CopyOperation().withDestinationKey(getInputParameter(S3_COPY_TO_FILENAME))
          .withBucketName(getInputParameter(S3_BUCKETNAME)).withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service copyService = build(copy);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      ServiceCase.execute(copyService, msg);

      S3GetOperation download = new S3GetOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service downloadService = build(download);
      ServiceCase.execute(downloadService, msg);
      assertEquals(MSG_CONTENTS, msg.getContent());
    } else {
      System.err.println("localstack tests disabled; not executing test_08_Copy_WithoutDestinationBucket");
    }
  }

  @Test
  public void test_10_DeleteCopy() throws Exception {
    if (areTestsEnabled()) {
      DeleteOperation delete = new DeleteOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service service = build(delete);
      ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());

      CheckFileExistsOperation check = new CheckFileExistsOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_COPY_TO_FILENAME));
      S3Service checker = build(check);
      try {
        ServiceCase.execute(checker, AdaptrisMessageFactory.getDefaultInstance().newMessage());
        fail();
      }
      catch (ServiceException expected) {

      }
    } else {
      System.err.println("localstack tests disabled; not executing test_09_DeleteCopy");
    }
  }

  @Test
  public void test_11_DeleteUploaded() throws Exception {
    if (areTestsEnabled()) {
      DeleteOperation delete = new DeleteOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service service = build(delete);
      ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());

      CheckFileExistsOperation check = new CheckFileExistsOperation().withBucketName(getInputParameter(S3_BUCKETNAME))
          .withKey(getInputParameter(S3_UPLOAD_FILENAME));
      S3Service checker = build(check);
      try {
        ServiceCase.execute(checker, AdaptrisMessageFactory.getDefaultInstance().newMessage());
        fail();
      }
      catch (ServiceException expected) {

      }
    } else {
      System.err.println("localstack tests disabled; not executing test_10_DeleteUploaded");
    }
  }


  @Test
  public void test_99_DeleteBucket() throws Exception {
    if (areTestsEnabled()) {
      DeleteBucketOperation delete = new DeleteBucketOperation().withBucketName(getInputParameter(S3_BUCKETNAME));
      S3Service service = build(delete);
      ServiceCase.execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
    } else {
      System.err.println("localstack tests disabled; not executing test_99_DeleteBucket");
    }
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

  protected DataInputParameter<String> getInputParameter(String cfgKey) {
    String name = config.getProperty(cfgKey);
    return new ConstantDataInputParameter(name);
  }

}
