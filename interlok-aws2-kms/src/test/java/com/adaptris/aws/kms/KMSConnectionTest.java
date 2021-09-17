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

package com.adaptris.aws.kms;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Test;
import com.adaptris.aws.AWSKeysAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;

public class KMSConnectionTest {

  @Test
  public void testCreateBuilder() throws Exception {
    AWSKMSConnection c = new AWSKMSConnection();
    assertNotNull(c.createBuilder());
    c.setCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
    assertNotNull(c.createBuilder());
    // This will throw a SecurityException
    try {
      c.setCredentials(
          new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "PW:BLAH_BLAH_BLAH_BLAH")));
      c.createBuilder();
      fail();
    } catch (CoreException expected) {

    }

  }

  @Test
  public void testLifecycle() throws Exception {
    AWSKMSConnection c = new AWSKMSConnection();
    try {
      c.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(c);
      assertNotNull(c.awsClient());
    } finally {
      LifecycleHelper.stopAndClose(c);
    }
    assertNull(c.awsClient());
    ClientWrapper.shutdownQuietly(c.awsClient());
  }
}
