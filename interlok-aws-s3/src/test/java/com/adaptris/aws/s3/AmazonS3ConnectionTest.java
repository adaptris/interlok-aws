package com.adaptris.aws.s3;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.PluggableRetryPolicyFactory;
import com.adaptris.core.BaseCase;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.KeyValuePairSet;

public class AmazonS3ConnectionTest extends BaseCase {

  public AmazonS3ConnectionTest(String name) {
    super(name);
  }


  public void testRegion() {
    AmazonS3Connection c = new AmazonS3Connection();
    assertNull(c.getRegion());
    c.setRegion("eu-central-1");
    assertEquals("eu-central-1", c.getRegion());
  }

  public void testAuthentication() {
    AmazonS3Connection c = new AmazonS3Connection();
    assertNull(c.getAuthentication());
    assertEquals(DefaultAWSAuthentication.class, c.authentication().getClass());
    AWSKeysAuthentication auth = new AWSKeysAuthentication("access", "secret");
    c.setAuthentication(auth);
    assertEquals(auth, c.getAuthentication());
    assertEquals(auth, c.authentication());
  }

  public void testClientConfiguration() {
    AmazonS3Connection c = new AmazonS3Connection();
    assertNull(c.getClientConfiguration());
    assertEquals(0, c.clientConfiguration().size());
    c.setClientConfiguration(new KeyValuePairSet());
    assertNotNull(c.getClientConfiguration());
    assertEquals(0, c.clientConfiguration().size());
  }

  public void testRetryPolicy() {
    AmazonS3Connection c = new AmazonS3Connection();
    assertNull(c.getRetryPolicy());
    assertEquals(DefaultRetryPolicyFactory.class, c.retryPolicy().getClass());
    c.setRetryPolicy(new PluggableRetryPolicyFactory());
    assertNotNull(c.getRetryPolicy());
    assertEquals(PluggableRetryPolicyFactory.class, c.retryPolicy().getClass());
  }

  public void testLifecycle() throws Exception {
    AmazonS3Connection c = new AmazonS3Connection();
    try {
      c.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(c);
      assertNotNull(c.amazonClient());
      assertNotNull(c.transferManager());
    } finally {
      LifecycleHelper.stopAndClose(c);
    }
    assertNull(c.amazonClient());
    assertNull(c.transferManager());
  }
}
