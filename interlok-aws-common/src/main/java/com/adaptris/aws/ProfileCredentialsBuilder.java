package com.adaptris.aws;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * Credentials provider based on AWS configuration profiles
 * 
 * @config aws-profile-credentials-builder
 */
@XStreamAlias("aws-profile-credentials-builder")
@ComponentProfile(summary = "Credentials provider based on AWS configuration profiles")
public class ProfileCredentialsBuilder implements AWSCredentialsProviderBuilder {

  /**
   * The profile configuration file.
   * <p>
   * If not specified then the behaviour is down to the AWS SDK. This probably means that
   * {@code ~/.aws/config} is used as the configuration file.
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
  public AWSCredentialsProvider build() throws Exception {
    ProfileCredentialsProvider credentials = new ProfileCredentialsProvider(configFile(), getProfileName());
    if (getRefreshIntervalNanos() != null) {
      credentials.setRefreshIntervalNanos(getRefreshIntervalNanos().longValue());
      credentials.setRefreshForceIntervalNanos(getRefreshIntervalNanos().longValue() * 2);
    }
    return credentials;
  }

  private ProfilesConfigFile configFile() {
    return getConfigFile() != null ? new ProfilesConfigFile(getConfigFile()) : null;
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
