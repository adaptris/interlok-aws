package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class StaticCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    StaticCredentialsBuilder auth = new StaticCredentialsBuilder();
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(DefaultCredentialsProvider.create(), provider);
  }

  @Test
  public void testBuild() throws Exception {
    StaticCredentialsBuilder auth = new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey"));
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertNotSame(AnonymousCredentialsProvider.create(), provider);
    assertEquals(StaticCredentialsProvider.class, provider.getClass());
  }
}
