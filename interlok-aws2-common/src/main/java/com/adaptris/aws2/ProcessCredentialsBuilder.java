package com.adaptris.aws2;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProcessCredentialsProvider;

import javax.validation.constraints.NotBlank;
import java.time.Duration;

/**
 * Credentials provider that can load credentials from an external process.
 *
 * <p>
 * See
 * https://docs.aws.amazon.com/cli/latest/topic/config-vars.html#sourcing-credentials-from-external-processes
 * for more information
 * </p>
 *
 * @config aws2-process-credentials-builder
 *
 */
@XStreamAlias("aws2-process-credentials-builder")
@ComponentProfile(summary = "Credentials provider that can load credentials from an external process", since = "3.9.2")
public class ProcessCredentialsBuilder implements AWSCredentialsProviderBuilder {

  /**
   * The command that should be executed to retrieve credentials including arguments
   * <p>
   * e.g. if you have a custom aws2 credentials program that takes arguments then you could have
   * {@code /opt/bin/awscreds-custom --username interlok} here. The program is expected to output JSON
   * data as per the <a href=
   * "https://docs.aws.amazon.com/cli/latest/topic/config-vars.html#sourcing-credentials-from-external-processes">documentation</a>
   * </p>
   */
  @Getter
  @Setter
  @NotBlank
  @NonNull
  private String command;


  /**
   * The maximum amount of data that can be returned by the external process before an exception is
   * raised.
   * <p>
   * If not specified, then the AWS internal default is used (currently 1024 bytes).
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "null")
  @Getter
  @Setter
  private Long processOutputLimitBytes;

  /**
   * The number of seconds between when the credentials expire and when the credentials should start
   * to be refreshed.
   *
   * <p>
   * This setting allows the credentials to be refreshed <strong>before</strong> they are reported to
   * expire. If not configured, then the AWS internal default is used (currently 15 seconds)
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "null")
  @Getter
  @Setter
  private Integer expirationBufferSeconds;

  public ProcessCredentialsBuilder() {
  }

  @Override
  public AwsCredentialsProvider build() throws Exception {
    ProcessCredentialsProvider.Builder builder = ProcessCredentialsProvider.builder().command(getCommand());
    if (getProcessOutputLimitBytes() != null) {
      builder.processOutputLimit(getProcessOutputLimitBytes().longValue());
    }
    if (getExpirationBufferSeconds() != null) {
      builder.credentialRefreshThreshold(Duration.ofSeconds(getExpirationBufferSeconds().intValue()));
    }
    return builder.build();
  }


  public ProcessCredentialsBuilder withCommand(String s) {
    setCommand(s);
    return this;
  }

  public ProcessCredentialsBuilder withProcessOutputLimitBytes(Long l) {
    setProcessOutputLimitBytes(l);
    return this;
  }

  public ProcessCredentialsBuilder withExpirationBufferSeconds(Integer i) {
    setExpirationBufferSeconds(i);
    return this;
  }

}
