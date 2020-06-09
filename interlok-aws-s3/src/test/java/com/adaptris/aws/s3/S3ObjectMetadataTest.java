package com.adaptris.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import com.adaptris.aws.s3.meta.S3ContentDisposition;
import com.adaptris.aws.s3.meta.S3ContentEncoding;
import com.adaptris.aws.s3.meta.S3ContentLanguage;
import com.adaptris.aws.s3.meta.S3ContentType;
import com.adaptris.aws.s3.meta.S3ExpirationTimeRuleId;
import com.adaptris.aws.s3.meta.S3HttpExpiresDate;
import com.adaptris.aws.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.util.TimeInterval;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3ObjectMetadataTest {

  @Test
  public void testContentDisposition() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3ContentDisposition om = new S3ContentDisposition();
    om.setContentDisposition("content disposition");
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertEquals("content disposition", meta.getContentDisposition());
  }


  @Test
  public void testContentLanguage() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3ContentLanguage om = new S3ContentLanguage();
    om.setContentLanguage("content language");
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertEquals("content language", meta.getContentLanguage());
  }

  @Test
  public void testContentType() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3ContentType om = new S3ContentType();
    om.setContentType("content type");
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertEquals("content type", meta.getContentType());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testExpirationRuleId() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3ExpirationTimeRuleId om = new S3ExpirationTimeRuleId();
    om.setExpirationRuleId("deprecated expiration rule");
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertEquals("deprecated expiration rule", meta.getExpirationTimeRuleId());
    om.setExpirationRuleId(null);
    om.setExpirationTimeRuleId("proper rule id");
    om.apply(msg, meta);
    assertEquals("proper rule id", meta.getExpirationTimeRuleId());
  }

  @Test
  public void testExpiresDate() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3HttpExpiresDate om = new S3HttpExpiresDate();
    om.setTimeToLive(new TimeInterval(10L, TimeUnit.SECONDS));
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    Date expirationDate = meta.getHttpExpiresDate();
    assertTrue("Expiration date too small",
        expirationDate.getTime() > (new Date().getTime() + 9000));
    assertTrue("Expiration date too large",
        expirationDate.getTime() < (new Date().getTime() + 11000));
  }


  @Test
  public void testContentEncoding() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3ContentEncoding om = new S3ContentEncoding();
    om.setContentEncoding("content encoding");
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertEquals("content encoding", meta.getContentEncoding());
  }

  @Test
  public void testServersideEncryption() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    S3ServerSideEncryption om = new S3ServerSideEncryption();
    ObjectMetadata meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, meta.getSSEAlgorithm());
    om.setEnabled(false);
    meta = new ObjectMetadata();
    om.apply(msg, meta);
    assertNull(meta.getSSEAlgorithm());
  }

}
