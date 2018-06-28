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

package com.adaptris.aws.sns;

import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.PluggableRetryPolicyFactory;
import com.adaptris.aws.sns.AmazonSNSConnection;
import com.adaptris.core.BaseCase;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.util.KeyValuePairSet;

public class AmazonSNSConnectionTest extends BaseCase {

  public AmazonSNSConnectionTest(String name) {
    super(name);
  }


  public void testRegion() {
    AmazonSNSConnection c = new AmazonSNSConnection();
    assertNull(c.getRegion());
    c.setRegion("eu-central-1");
    assertEquals("eu-central-1", c.getRegion());
  }

  public void testAuthentication() {
    AmazonSNSConnection c = new AmazonSNSConnection();
    assertNull(c.getAuthentication());
    assertEquals(DefaultAWSAuthentication.class, c.authentication().getClass());
    AWSKeysAuthentication auth = new AWSKeysAuthentication("access", "secret");
    c.setAuthentication(auth);
    assertEquals(auth, c.getAuthentication());
    assertEquals(auth, c.authentication());
  }

  public void testClientConfiguration() {
    AmazonSNSConnection c = new AmazonSNSConnection();
    assertNull(c.getClientConfiguration());
    assertEquals(0, c.clientConfiguration().size());
    c.setClientConfiguration(new KeyValuePairSet());
    assertNotNull(c.getClientConfiguration());
    assertEquals(0, c.clientConfiguration().size());
  }

  public void testRetryPolicy() {
    AmazonSNSConnection c = new AmazonSNSConnection();
    assertNull(c.getRetryPolicy());
    assertEquals(DefaultRetryPolicyFactory.class, c.retryPolicy().getClass());
    c.setRetryPolicy(new PluggableRetryPolicyFactory());
    assertNotNull(c.getRetryPolicy());
    assertEquals(PluggableRetryPolicyFactory.class, c.retryPolicy().getClass());
  }

  public void testLifecycle() throws Exception {
    AmazonSNSConnection c = new AmazonSNSConnection();
    try {
      c.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(c);
      assertNotNull(c.amazonClient());
    } finally {
      LifecycleHelper.stopAndClose(c);
    }
    assertNull(c.amazonClient());
  }
}
