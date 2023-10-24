package com.adaptris.aws2;

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProcessCredentialsProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ProcessCredentialsBuilderTest {

  @Test(expected = NullPointerException.class)
  public void testBuild_Defaults() throws Exception {
    ProcessCredentialsBuilder auth = new ProcessCredentialsBuilder();
    AwsCredentialsProvider provider = auth.build();
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
