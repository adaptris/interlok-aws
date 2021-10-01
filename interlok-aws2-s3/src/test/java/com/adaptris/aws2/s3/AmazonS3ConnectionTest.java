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

package com.adaptris.aws2.s3;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class AmazonS3ConnectionTest extends BaseCase {

  @Test
  public void testCreateBuilder() throws Exception {
    AmazonS3Connection c = new AmazonS3Connection();
    assertNotNull(c.createBuilder());
    c.setForcePathStyleAccess(Boolean.TRUE);
    assertNotNull(c.createBuilder());
    c.setForcePathStyleAccess(null);
    c.setCredentials(StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey")));
    assertNotNull(c.createBuilder());

    // This will throw a SecurityException
    try {
      c.setCredentials(StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "PW:BLAH_BLAH_BLAH_BLAH")));
      c.createBuilder();
      fail();
    } catch (CoreException expected) {

    }

  }

  @Test
  public void testLifecycle() throws Exception {
    AmazonS3Connection c = new AmazonS3Connection();
    try {
      c.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(c);
      assertNotNull(c.amazonClient());
    } finally {
      LifecycleHelper.stopAndClose(c);
    }
    assertNull(c.amazonClient());
    AmazonS3Connection.shutdownQuietly(c.amazonClient());
  }
}
