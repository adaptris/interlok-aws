package com.adaptris.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ProcessCredentialsProvider;

public class ProcessCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    ProcessCredentialsBuilder auth = new ProcessCredentialsBuilder();
    assertThrows(IllegalArgumentException.class, ()->{
      AWSCredentialsProvider provider = auth.build();
    }, "Failed to build");
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
