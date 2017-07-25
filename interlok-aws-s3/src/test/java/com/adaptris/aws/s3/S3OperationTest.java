package com.adaptris.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.MetadataElement;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.adaptris.interlok.InterlokException;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3OperationTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testKey() {
    MyS3Operation op = new MyS3Operation();
    assertNull(op.getKey());
    op.setKey(new ConstantDataInputParameter("hello"));
    assertEquals(ConstantDataInputParameter.class, op.getKey().getClass());
    try {
      op.setKey(null);
      fail();
    }
    catch (IllegalArgumentException expected) {

    }
    assertEquals(ConstantDataInputParameter.class, op.getKey().getClass());
  }

  @Test
  public void testBucket() {
    MyS3Operation op = new MyS3Operation();
    assertNull(op.getBucketName());
    op.setBucketName(new ConstantDataInputParameter("hello"));
    assertEquals(ConstantDataInputParameter.class, op.getBucketName().getClass());
    try {
      op.setBucketName(null);
      fail();
    }
    catch (IllegalArgumentException expected) {

    }
    assertEquals(ConstantDataInputParameter.class, op.getBucketName().getClass());
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
    MyS3Operation op = new MyS3Operation();
    Map<String, String> result = op.filterMetadata(msg);
    assertEquals(0, result.size());
  }

  @Test
  public void testFilterToMetadataCollection() {
    Map<String, String> map = new HashMap<>();
    map.put("hello", "world");
    MyS3Operation op = new MyS3Operation();
    Set<MetadataElement> result = op.filterUserMetadata(map);
    assertEquals(0, result.size());
  }

  private static class MyS3Operation extends S3OperationImpl {

    @Override
    public void execute(AmazonS3Client s3, AdaptrisMessage msg) throws InterlokException {
      return;
    }

  }
}
