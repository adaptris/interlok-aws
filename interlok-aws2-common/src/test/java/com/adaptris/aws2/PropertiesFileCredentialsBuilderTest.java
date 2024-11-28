package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.adaptris.core.stubs.TempFileUtils;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class PropertiesFileCredentialsBuilderTest {

  @Test
  public void testBuild_Defaults() throws Exception {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder();
      AwsCredentialsProvider provider = auth.build();
    });    
  }

  @Test
  public void testBuild_File() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder();
    File file = createCredentials(auth);
    auth.withPropertyFile(file.getCanonicalPath());
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
  }

  @Test
  public void testBuild_Classpath() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder().withPropertyFile("classpath-credentials.properties");
    AwsCredentialsProvider provider = auth.build();
    assertNotNull(provider);
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
