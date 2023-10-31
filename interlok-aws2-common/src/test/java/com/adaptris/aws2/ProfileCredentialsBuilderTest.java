package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.Test;

import com.adaptris.core.stubs.TempFileUtils;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

public class ProfileCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    ProfileCredentialsBuilder auth = new ProfileCredentialsBuilder();
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ProfileCredentialsProvider.class, provider.getClass());
  }

  @Test
  public void testBuild_Profile() throws Exception {
    ProfileCredentialsBuilder auth = new ProfileCredentialsBuilder();
    File configFile = createCredentials(auth);
    auth.withConfigFile(configFile.getCanonicalPath())
        .withProfileName("external").withRefreshIntervalNanos(5L * 60 * 1000 * 1000);
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ProfileCredentialsProvider.class, provider.getClass());
  }

  private static File createCredentials(Object tracker) throws Exception {
    File result = TempFileUtils.createTrackedFile(tracker);
    try (PrintStream pw = new PrintStream(new FileOutputStream(result))) {
      pw.println("[default]");
      pw.println("aws_access_key_id = aws_access_key");
      pw.println("aws_secret_access_key = aws_secret_key");
      pw.println("[profile external]");
      pw.println("credential_process = /opt/bin/awscreds-custom");
    }
    return result;
  }
}
