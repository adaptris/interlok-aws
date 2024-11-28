package com.adaptris.aws2;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import javax.validation.constraints.NotBlank;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Credentials provider that loads credentials from a property file either from the filesystem or
 * classpath.
 *
 * <p>
 * This class uses either {@code com.amazonaws.auth.PropertiesFileCredentialsProvider} or
 * {@code com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider} depending on whether the
 * specified file exists or not; if the file exists then we use
 * {@code PropertiesFileCredentialsProvider} otherwise it is assumed to be on the classpath and we
 * use {@code ClasspathPropertiesFileCredentialsProvider}.
 * </p>
 *
 * <p>
 * The documentation for those two classes should be considered canonical, but essentially the AWS
 * access key ID is expected to be in the <code>accessKey</code> property and the AWS secret key is
 * expected to be in the <code>secretKey</code> property.
 * </p>
 *
 * @config aws2-properties-file-credentials-builder
 * @since 4.3.0
 *
 */
@XStreamAlias("aws2-properties-file-credentials-builder")
@ComponentProfile(
    summary = "Credentials provider that loads credentials from a property file either from the filesystem or classpath",
    since = "4.3.0")
public class PropertiesFileCredentialsBuilder implements AWSCredentialsProviderBuilder {

  /**
   * The property file that contains the credentials
   */
  @Getter
  @Setter
  @NotBlank
  @NonNull
  private String propertyFile;

  public PropertiesFileCredentialsBuilder() {
  }

  @Override
  public AwsCredentialsProvider build() throws Exception {
    File file = new File(Args.notBlank(getPropertyFile(), "property-file"));

    Properties properties = new Properties();

    if (isReadable(file))
    {
      try (FileInputStream stream = new FileInputStream(file))
      {
        properties.load(stream);
      }
    }
    else
    {
      String credentialsFilePath = getPropertyFile();
      if (!credentialsFilePath.startsWith("/"))
      {
        credentialsFilePath = "/" + credentialsFilePath;
      }
      try (InputStream inputStream = getClass().getResourceAsStream(credentialsFilePath))
      {
        if (inputStream == null)
        {
          throw new IOException("Unable to load AWS credentials from the " + credentialsFilePath + " file on the classpath");
        }
        properties.load(inputStream);
      }
    }

    if (properties.getProperty("accessKey") == null || properties.getProperty("secretKey") == null)
    {
      throw new IllegalArgumentException("The specified properties data doesn't contain the expected properties 'accessKey' and 'secretKey'.");
    }
    final String accessKey = properties.getProperty("accessKey");
    final String secretAccessKey = properties.getProperty("secretKey");

    return () -> AwsBasicCredentials.create(accessKey, secretAccessKey);
  }


  private boolean isReadable(File f) {
    return BooleanUtils.and(new boolean[] {f.exists(), f.isFile(), f.canRead()});
  }

  public PropertiesFileCredentialsBuilder withPropertyFile(String s) {
    setPropertyFile(s);
    return this;
  }

}
