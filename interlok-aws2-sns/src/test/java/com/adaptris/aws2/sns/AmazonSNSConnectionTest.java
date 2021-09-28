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

package com.adaptris.aws2.sns;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import com.amazonaws.services.sns.AmazonSNSClient;

public class AmazonSNSConnectionTest extends BaseCase {

  @Test
  public void testRegion() {
    AmazonSNSConnection c = new AmazonSNSConnection();
    assertNull(c.getRegion());
    c.setRegion("eu-central-1");
    assertEquals("eu-central-1", c.getRegion());
  }

  @Test
  public void testLifecycle() throws Exception {
    AmazonSNSConnection conn = new AmazonSNSConnection().withCredentialsProviderBuilder(
        new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("access", "secret")));
    try {
      conn.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(conn);
      assertNotNull(conn.amazonClient());
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
    assertNull(conn.amazonClient());
    conn = new AmazonSNSConnection();
    try {
      conn.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(conn);
      assertNotNull(conn.amazonClient());
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
    assertNull(conn.amazonClient());

    CustomEndpoint customEndpoint = Mockito.mock(CustomEndpoint.class);
    Mockito.when(customEndpoint.rebuild(any())).thenThrow(new RuntimeException());
    Mockito.when(customEndpoint.isConfigured()).thenReturn(true);
    conn = new AmazonSNSConnection();
    conn.setCustomEndpoint(customEndpoint);
    try {
      conn.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(conn);
      fail();
    } catch (CoreException expected) {

    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
    assertNull(conn.amazonClient());

  }

  @Test
  public void testCloseQuietly() throws Exception {
    AmazonSNSClient mockClient= Mockito.mock(AmazonSNSClient.class);
    AmazonSNSConnection.closeQuietly(null);
    AmazonSNSConnection.closeQuietly(mockClient);
  }

}
