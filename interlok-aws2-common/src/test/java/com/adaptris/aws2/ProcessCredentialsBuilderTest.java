package com.adaptris.aws2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ProcessCredentialsProvider;

public class ProcessCredentialsBuilderTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBuild_Defaults() throws Exception {
    ProcessCredentialsBuilder auth = new ProcessCredentialsBuilder();
    AWSCredentialsProvider provider = auth.build();
  }

  @Test
  public void testBuild() throws Exception {
    ProcessCredentialsBuilder auth =
        new ProcessCredentialsBuilder().withCommand("ls").withExpirationBufferSeconds(15).withProcessOutputLimitBytes(1024l);
    AWSCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ProcessCredentialsProvider.class, provider.getClass());
  }
}
