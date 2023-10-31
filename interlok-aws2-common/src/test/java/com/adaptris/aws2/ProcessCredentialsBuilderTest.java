package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProcessCredentialsProvider;

public class ProcessCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    Assertions.assertThrows(NullPointerException.class, () -> {
      ProcessCredentialsBuilder auth = new ProcessCredentialsBuilder();
      AwsCredentialsProvider provider = auth.build();
    });
  }

  @Test
  public void testBuild() throws Exception {
    ProcessCredentialsBuilder auth =
        new ProcessCredentialsBuilder().withCommand("ls").withExpirationBufferSeconds(15).withProcessOutputLimitBytes(1024l);
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ProcessCredentialsProvider.class, provider.getClass());
  }
}
