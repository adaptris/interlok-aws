package com.adaptris.aws;

import java.util.concurrent.TimeUnit;
import org.hibernate.validator.constraints.NotBlank;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ProcessCredentialsProvider;
import com.amazonaws.auth.ProcessCredentialsProvider.Builder;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Credentials provider that can load credentials from an external process.
 * 
 * <p>
 * See
 * https://docs.aws.amazon.com/cli/latest/topic/config-vars.html#sourcing-credentials-from-external-processes
 * for more information
 * </p>
 * 
 * @config aws-process-credentials-builder
 * 
 */
@XStreamAlias("aws-process-credentials-builder")
@ComponentProfile(summary = "Credentials provider that can load credentials from an external process", since = "3.9.2")
public class ProcessCredentialsBuilder implements AWSCredentialsProviderBuilder {

  /**
   * The command that should be executed to retrieve credentials including arguments
   * <p>
   * e.g. if you have a custom aws credentials program that takes arguments then you could have
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
  public AWSCredentialsProvider build() throws Exception {
    Builder builder = ProcessCredentialsProvider.builder().withCommand(getCommand());
    if (getProcessOutputLimitBytes() != null) {
      builder.withProcessOutputLimit(getProcessOutputLimitBytes().longValue());
    }
    if (getExpirationBufferSeconds() != null) {
      builder.withCredentialExpirationBuffer(getExpirationBufferSeconds().intValue(), TimeUnit.SECONDS);
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
