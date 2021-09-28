package com.adaptris.aws2;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

import java.io.File;

/**
 * Credentials provider based on AWS configuration profiles
 * 
 * @config aws2-profile-credentials-builder
 */
@XStreamAlias("aws2-profile-credentials-builder")
@ComponentProfile(summary = "Credentials provider based on AWS configuration profiles",
    since = "3.9.2")
public class ProfileCredentialsBuilder implements AWSCredentialsProviderBuilder {

  /**
   * The profile configuration file.
   * <p>
   * If not specified then the behaviour is down to the AWS SDK. This probably means that
   * {@code ~/.aws2/config} is used as the configuration file.
   * </p>
   */
  @InputFieldDefault(value = "null")
  @Getter
  @Setter
  private String configFile;

  /**
   * The profile within configuration.
   * <p>
   * If not specified then the behaviour is down to the AWS SDK. This will probably mean the default
   * profile.
   * </p>
   */
  @InputFieldDefault(value = "null")
  @Getter
  @Setter
  private String profileName;

  /**
   * The refresh interval in nano seconds.
   * <p>
   * This maps onto {@code ProfileCredentialsProvider#setRefreshIntervalNanos(long}}. The
   * corresponding {@code ProfileCredentialsProvider#setRefreshForceIntervalNanos(long)} is set to 2x
   * whatever you have configured here.
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "null")
  @Getter
  @Setter
  private Long refreshIntervalNanos;

  public ProfileCredentialsBuilder() {
  }

  @Override
  public AwsCredentialsProvider build() throws Exception {

    ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
    builder.profileFile(configFile()).profileName(profileName);
    return builder.build();
  }

  private ProfileFile configFile() {
    if (configFile == null) {
      return null;
    }
    ProfileFile.Builder builder = ProfileFile.builder();
    builder.content(new File(configFile).toPath());
    return builder.build();
  }

  public ProfileCredentialsBuilder withConfigFile(String s) {
    setConfigFile(s);
    return this;
  }

  public ProfileCredentialsBuilder withProfileName(String s) {
    setProfileName(s);
    return this;
  }

  public ProfileCredentialsBuilder withRefreshIntervalNanos(Long l) {
    setRefreshIntervalNanos(l);
    return this;
  }
}
