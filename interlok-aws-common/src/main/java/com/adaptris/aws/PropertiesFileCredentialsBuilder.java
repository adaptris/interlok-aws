package com.adaptris.aws;

import java.io.File;
import org.apache.commons.lang3.BooleanUtils;
import org.hibernate.validator.constraints.NotBlank;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.util.Args;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

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
 * @config aws-properties-file-credentials-builder
 * 
 */
@XStreamAlias("aws-properties-file-credentials-builder")
@ComponentProfile(
    summary = "Credentials provider that loads credentials from a property file either from the filesystem or classpath",
    since = "3.9.2")
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
  public AWSCredentialsProvider build() throws Exception {
    File file = new File(Args.notBlank(getPropertyFile(), "property-file"));
    if (isReadable(file)) {
      return new PropertiesFileCredentialsProvider(file.getCanonicalPath());
    }
    return new ClasspathPropertiesFileCredentialsProvider(getPropertyFile());
  }


  private boolean isReadable(File f) {
    return BooleanUtils.and(new boolean[] {f.exists(), f.isFile(), f.canRead()});
  }

  public PropertiesFileCredentialsBuilder withPropertyFile(String s) {
    setPropertyFile(s);
    return this;
  }

}
