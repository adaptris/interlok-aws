package com.adaptris.aws2;

import com.adaptris.core.stubs.TempFileUtils;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PropertiesFileCredentialsBuilderTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBuild_Defaults() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder();
    AwsCredentialsProvider provider = auth.build();
  }

  @Test
  public void testBuild_File() throws Exception {
    PropertiesFileCredentialsBuilder auth =
        new PropertiesFileCredentialsBuilder();
    File file = createCredentials(auth);
    auth.withPropertyFile(file.getCanonicalPath());
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ProfileCredentialsProvider.class, provider.getClass());
  }

  @Test
  public void testBuild_Classpath() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder().withPropertyFile("classpath-credentials.properties");
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ProfileCredentialsProvider.class, provider.getClass());
  }

  private static File createCredentials(Object tracker) throws Exception {
    File result = TempFileUtils.createTrackedFile(tracker);
    try (PrintStream pw = new PrintStream(new FileOutputStream(result))) {
      pw.println("accessKey=MyAccessKey");
      pw.println("secretKey=MySecretKey");
      pw.println("other=properties");
      pw.println("that=are_ignored");
    }
    return result;
  }
}
