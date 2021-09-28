package com.adaptris.aws2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import org.junit.Test;
import com.adaptris.core.stubs.TempFileUtils;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;

public class PropertiesFileCredentialsBuilderTest {

  @Test(expected = IllegalArgumentException.class)
  public void testBuild_Defaults() throws Exception {
    PropertiesFileCredentialsBuilder auth = new PropertiesFileCredentialsBuilder();
    AWSCredentialsProvider provider = auth.build();
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
