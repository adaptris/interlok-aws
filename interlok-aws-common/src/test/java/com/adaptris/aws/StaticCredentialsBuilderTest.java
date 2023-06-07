package com.adaptris.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import org.junit.jupiter.api.Test;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class StaticCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    StaticCredentialsBuilder auth = new StaticCredentialsBuilder();
    AWSCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(DefaultAWSCredentialsProviderChain.getInstance(), provider);
  }

  @Test
  public void testBuild() throws Exception {
    StaticCredentialsBuilder auth =
        new StaticCredentialsBuilder().withAuthentication(new AWSKeysAuthentication("accessKey", "secretKey"));
    AWSCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertNotSame(DefaultAWSCredentialsProviderChain.getInstance(), provider);
    assertEquals(AWSStaticCredentialsProvider.class, provider.getClass());
  }
}
