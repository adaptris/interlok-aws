/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import com.adaptris.aws.s3.meta.S3ContentDisposition;
import com.adaptris.aws.s3.meta.S3ContentEncoding;
import com.adaptris.aws.s3.meta.S3ContentLanguage;
import com.adaptris.aws.s3.meta.S3ContentType;
import com.adaptris.aws.s3.meta.S3ExpirationTimeRuleId;
import com.adaptris.aws.s3.meta.S3HttpExpiresDate;
import com.adaptris.aws.s3.meta.S3ObjectMetadata;
import com.adaptris.aws.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.adaptris.interlok.InterlokException;
import com.adaptris.util.TimeInterval;
import com.amazonaws.services.s3.model.ObjectMetadata;

@SuppressWarnings("deprecation")
public class S3OperationTest {


  @Test
  public void testKey() {
    MyS3Operation op = new MyS3Operation();
    assertNull(op.getKey());
    op.withKey(new ConstantDataInputParameter("hello"));
    assertEquals(ConstantDataInputParameter.class, op.getKey().getClass());
    assertNull(op.getObjectName());
    op.withObjectName("hello");
    assertEquals("hello", op.getObjectName());
  }

  @Test
  public void testBucket() {
    MyS3Operation op = new MyS3Operation();
    assertNull(op.getBucketName());
    op.withBucketName(new ConstantDataInputParameter("hello"));
    assertEquals(ConstantDataInputParameter.class, op.getBucketName().getClass());
    op.setBucketName(null);
    assertNull(op.getBucket());
    op.withBucket("hello");
    assertEquals("hello", op.getBucket());
  }

  @Test
  public void testMetadataFilter() {
    MyS3Operation op = new MyS3Operation();
    assertNull(op.getUserMetadataFilter());
    assertEquals(RemoveAllMetadataFilter.class, op.userMetadataFilter().getClass());
    op.setUserMetadataFilter(new NoOpMetadataFilter());
    assertEquals(NoOpMetadataFilter.class, op.getUserMetadataFilter().getClass());
    assertEquals(NoOpMetadataFilter.class, op.getUserMetadataFilter().getClass());
    op.setUserMetadataFilter(null);
    assertNull(op.getUserMetadataFilter());
    assertEquals(RemoveAllMetadataFilter.class, op.userMetadataFilter().getClass());
  }

  @Test
  public void testFilterFromAdaptrisMessage() {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMetadata(new MetadataElement("hello", "world"));
    MyS3Operation op = new MyS3Operation().withUserMetadataFilter(new NoOpMetadataFilter());
    Map<String, String> result = op.filterMetadata(msg);
    assertEquals(1, result.size());
  }

  @Test
  public void testFilterToMetadataCollection() {
    Map<String, String> map = new HashMap<>();
    map.put("hello", "world");
    MyS3Operation op = new MyS3Operation();
    Set<MetadataElement> result = op.filterUserMetadata(map);
    assertEquals(0, result.size());
  }
  
  @Test
  public void testS3ObjectMetadata() throws UnsupportedEncodingException, ServiceException {
    List<S3ObjectMetadata> allmetas = new ArrayList<S3ObjectMetadata>(); 
    {
      S3ContentDisposition cd = new S3ContentDisposition();
      cd.setContentDisposition("content disposition");
      allmetas.add(cd);
    } {
      S3ContentLanguage cl = new S3ContentLanguage();
      cl.setContentLanguage("content language");
      allmetas.add(cl);
    } {
      S3ContentType ct = new S3ContentType();
      ct.setContentType("content type");
      allmetas.add(ct);
    } {
      S3ExpirationTimeRuleId eri = new S3ExpirationTimeRuleId();
      eri.setExpirationTimeRuleId("expiration time rule id");
      allmetas.add(eri);
    } {
      S3HttpExpiresDate hed = new S3HttpExpiresDate();
      hed.setTimeToLive(new TimeInterval(10L, TimeUnit.SECONDS));
      allmetas.add(hed);
    } {
      S3ContentEncoding ce = new S3ContentEncoding();
      ce.setContentEncoding("content encoding");
      allmetas.add(ce);
    }{
      allmetas.add(new S3ServerSideEncryption());
    }

    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("some content", "utf8");
    
    ObjectMetadata meta = new ObjectMetadata();
    for(S3ObjectMetadata m: allmetas) {
      m.apply(msg, meta);
    }
    
    assertEquals("content disposition", meta.getContentDisposition());
    assertEquals("content language", meta.getContentLanguage());
    assertEquals("content type", meta.getContentType());
    assertEquals("expiration time rule id", meta.getExpirationTimeRuleId());
    assertEquals("content encoding", meta.getContentEncoding());
    Date expirationDate = meta.getHttpExpiresDate();
    assertTrue("Expiration date too small", expirationDate.getTime() > (new Date().getTime() + 9000));
    assertTrue("Expiration date too large", expirationDate.getTime() < (new Date().getTime() + 11000));
    assertEquals(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION, meta.getSSEAlgorithm());
  }
  
  @Test
  public void testS3ObjectMetadataFromMessage() throws UnsupportedEncodingException, ServiceException {
    List<S3ObjectMetadata> allmetas = new ArrayList<S3ObjectMetadata>(); 
    {
      S3ContentDisposition cd = new S3ContentDisposition();
      cd.setContentDisposition("%message{cd}");
      allmetas.add(cd);
    } {
      S3ContentLanguage cl = new S3ContentLanguage();
      cl.setContentLanguage("%message{cl}");
      allmetas.add(cl);
    } {
      S3ContentType ct = new S3ContentType();
      ct.setContentType("%message{ct}");
      allmetas.add(ct);
    } {
      S3ExpirationTimeRuleId eri = new S3ExpirationTimeRuleId();
      eri.setExpirationTimeRuleId("%message{eri}");
      allmetas.add(eri);
    } {
      S3ContentEncoding ce = new S3ContentEncoding();
      ce.setContentEncoding("%message{ce}");
      allmetas.add(ce);
    }
    Collections.sort(allmetas);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    msg.addMetadata("cd", "content disposition");
    msg.addMetadata("cl", "content language");
    msg.addMetadata("ct", "content type");
    msg.addMetadata("eri", "expiration time rule id");
    msg.addMetadata("ce", "content encoding");

    ObjectMetadata meta = new ObjectMetadata();
    for(S3ObjectMetadata m: allmetas) {
      m.apply(msg, meta);
    }
    
    assertEquals("content disposition", meta.getContentDisposition());
    assertEquals("content language", meta.getContentLanguage());
    assertEquals("content type", meta.getContentType());
    assertEquals("expiration time rule id", meta.getExpirationTimeRuleId());
    assertEquals("content encoding", meta.getContentEncoding());
  }

  @Test
  public void testMustHaveEither() throws Exception {
    try {
      S3OperationImpl.mustHaveEither(null, null);
      fail();
    } catch (IllegalArgumentException e) {

    }
    S3OperationImpl.mustHaveEither(new ConstantDataInputParameter("hello"), null);
    S3OperationImpl.mustHaveEither(null, "hello");
    S3OperationImpl.mustHaveEither(new ConstantDataInputParameter("hello"), "hello");
  }

  private static class MyS3Operation extends TransferOperation {

    @Override
    public void execute(ClientWrapper s3, AdaptrisMessage msg) throws InterlokException {
      return;
    }

  }
}
