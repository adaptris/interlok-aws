package com.adaptris.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.Test;
import com.adaptris.core.stubs.TempFileUtils;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;

public class PropertiesFileCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder();
    assertThrows(IllegalArgumentException.class, ()->{
      AWSCredentialsProvider provider = auth.build();
    }, "Failed to build");
  }

  @Test
  public void testBuild_File() throws Exception {
    PropertiesFileCredentialsBuilder auth =
        new PropertiesFileCredentialsBuilder();
    File file = createCredentials(auth);
    auth.withPropertyFile(file.getCanonicalPath());
    AWSCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(PropertiesFileCredentialsProvider.class, provider.getClass());
  }

  @Test
  public void testBuild_Classpath() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder().withPropertyFile("classpath-credentials.properties");
    AWSCredentialsProvider provider = auth.build();
    assertNotNull(provider);
    assertEquals(ClasspathPropertiesFileCredentialsProvider.class, provider.getClass());
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
