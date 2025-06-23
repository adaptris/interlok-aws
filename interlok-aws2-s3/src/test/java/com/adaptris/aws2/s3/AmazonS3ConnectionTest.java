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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.adaptris.aws2.CustomEndpoint;
import com.adaptris.core.CoreException;
import org.junit.jupiter.api.Test;

import com.adaptris.aws2.AWSKeysAuthentication;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

public class AmazonS3ConnectionTest extends BaseCase {

  @Test
  public void testCreateBuilder() throws Exception {
    AmazonS3Connection c = new AmazonS3Connection();
    assertNotNull(c.createBuilder());
    c.setForcePathStyleAccess(Boolean.TRUE);
    assertNotNull(c.createBuilder());
    c.setForcePathStyleAccess(null);
    c.setCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
    assertNotNull(c.createBuilder());
  }

  @Test
  public void testLifecycle() throws Exception {
    AmazonS3Connection c = new AmazonS3Connection();
    try {
      c.setCredentials(new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey")));
      c.setRegion("eu-central-1");
      LifecycleHelper.initAndStart(c);
      assertNotNull(c.amazonClient());
    } finally {
      LifecycleHelper.stopAndClose(c);
    }
    assertNull(c.amazonClient());
    AmazonS3Connection.shutdownQuietly(c.amazonClient());
  }

  @Test
  public void testCreateBuilderWithCustomEndpoint() throws Exception {
    AmazonS3Connection conn = spy(new AmazonS3Connection());
    CustomEndpoint customEndpoint = mock(CustomEndpoint.class);

    when(customEndpoint.isConfigured()).thenReturn(true);
    when(customEndpoint.getServiceEndpoint()).thenReturn("http://localhost:9000");
    when(customEndpoint.getSigningRegion()).thenReturn("us-west-2");
    doReturn(customEndpoint).when(conn).getCustomEndpoint();

    S3ClientBuilder builder = conn.createBuilder();
    builder.credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    );
    try (S3Client client = builder.build()) {
      assertEquals(URI.create("http://localhost:9000"), client.serviceClientConfiguration().endpointOverride().get());
      assertEquals(Region.of("us-west-2"), client.serviceClientConfiguration().region());
    }
  }

  @Test
  public void testCreateBuilderWithCustomEndpointNotConfigured() throws Exception {
    AmazonS3Connection conn = spy(new AmazonS3Connection());
    CustomEndpoint customEndpoint = mock(CustomEndpoint.class);

    when(customEndpoint.isConfigured()).thenReturn(false);
    doReturn(customEndpoint).when(conn).getCustomEndpoint();

    S3ClientBuilder builder = conn.createBuilder();
    assertNotNull(builder);

    verify(customEndpoint, never()).getServiceEndpoint();
    verify(customEndpoint, never()).getSigningRegion();
  }

  @Test
  void testCreateBuilderThrowsException() {
    AmazonS3Connection conn = spy(new AmazonS3Connection());
    doThrow(new RuntimeException("Simulated")).when(conn).getRegion();

    CoreException ex = assertThrows(CoreException.class, conn::createBuilder);
    assertInstanceOf(RuntimeException.class, ex.getCause());
    assertEquals("Simulated", ex.getCause().getMessage());
  }
}
