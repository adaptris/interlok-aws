package com.adaptris.aws;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.Builder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Tag;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

/**
 * AWS credentials that makes use of AWS STS
 * <p>
 * AWS Security Token Service (STS) enables you to request temporary, limited-privilege credentials
 * for AWS Identity and Access Management (IAM) users or for users that you authenticate (federated
 * users). For more information about using this service, see
 * <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html">Temporary
 * Security Credentials</a>.
 * </p>
 *
 * @config aws-sts-assumerole-credentials-builder
 */
@XStreamAlias("aws-sts-assumerole-credentials-builder")
@ComponentProfile(summary = "Create a set of credentials via STS",
    tag = "amazon,aws,sts,assumerole", since = "3.12.0")
@DisplayOrder(order = {"roleArn", "roleSessionName", "roleExternalId", "roleDurationSeconds",
    "scopeDownPolicy", "credentials", "sessionTags", "transitiveTagKeys"})
@NoArgsConstructor
public class STSAssumeroleCredentialsBuilder implements AWSCredentialsProviderBuilder {

  /**
   * The underlying credentials used to access STS.
   *
   */
  @NotNull
  @Valid
  @InputFieldDefault(value = "aws-static-credentials-builder with default credentials")
  @Getter
  @Setter
  private AWSCredentialsProviderBuilder credentials;

  /**
   * The required roleArn parameter when starting a session.
   *
   */
  @NonNull
  @NotNull
  @Getter
  @Setter
  private String roleArn;
  /**
   * The required roleSessionName when starting a session.
   *
   */
  @NonNull
  @NotNull
  @Getter
  @Setter
  private String roleSessionName;
  /**
   * An external id used in the service call used to retrieve session credentials
   */
  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external = true)
  private String roleExternalId;

  /**
   * The transitive tag keys we want to pass to the assume role request
   * <p>
   * This defaults to {@code null} to avoid configuration clutter
   * </p>
   */
  @Getter
  @Setter
  @AdvancedConfig
  private List<String> transitiveTagKeys;
  /**
   * The collection of tags which we want to pass to the assume role request
   * <p>
   * This defaults to {@code null} to avoid configuration clutter
   * </p>
   */
  @Getter
  @Setter
  @AdvancedConfig
  private KeyValuePairSet sessionTags;
  /**
   * The duration for which we want to have an assumed role session to be active
   * <p>
   * This defaults to {@code null} to avoid configuration clutter
   * </p>
   */
  @Getter
  @Setter
  @AdvancedConfig
  private Integer roleDurationSeconds;
  /**
   * An IAM policy in JSON format to scope down permissions granted from the assume role.
   * <p>
   * This is passed though to
   * {@code STSAssumeRoleSessionCredentialsProvider.Builder#withScopeDownPolicy(String)} as-is with
   * no checking, is completely optional and defaults to {@code null} to avoid configuration clutter
   * </p>
   */
  @Getter
  @Setter
  @AdvancedConfig(rare = true)
  @InputFieldHint(style = "JSON")
  private String scopeDownPolicy;

  @Override
  public AWSCredentialsProvider build() throws Exception {
    return build(null);
  }

  @Override
  public AWSCredentialsProvider build(BuilderConfig conf) throws Exception {
    return configure(new Builder(getRoleArn(), getRoleSessionName()), securityToken(conf)).build();
  }

  private AWSSecurityTokenService securityToken(BuilderConfig conf) throws Exception {
    AWSCredentialsProviderBuilder longlived =
        AWSCredentialsProviderBuilder.defaultIfNull(getCredentials());
    AWSSecurityTokenServiceClientBuilder stsBuilder =
        AWSSecurityTokenServiceClientBuilder.standard();

    // additional builder setters from the API that we're not exposing.
    // stsBuilder.setClientSideMonitoringConfigurationProvider(csmConfig);
    // stsBuilder.setMetricsCollector(metrics);
    // stsBuilder.setMonitoringListener(monitoringListener);

    if (conf != null) {
      stsBuilder.setClientConfiguration(
          ClientConfigurationBuilder.build(conf.clientConfiguration(), conf.retryPolicy()));
      // CustomEndpoint has a EndpointConfiguration
      // Connection#setRegion gives us a RegionEndpoint which gives us the correct
      // EndpointConfiguration as well.
      //
      conf.endpointBuilder().rebuild(stsBuilder);
      stsBuilder.setCredentials(longlived.build(conf));
    } else {
      stsBuilder.setCredentials(longlived.build());
    }
    return stsBuilder.build();
  }

  private Builder configure(Builder builder, AWSSecurityTokenService sts) throws Exception {
    Optional.ofNullable(getRoleDurationSeconds()).ifPresent((seconds) -> builder.withRoleSessionDurationSeconds(seconds.intValue()));
    Optional.ofNullable(getScopeDownPolicy()).ifPresent((policy) -> builder.withScopeDownPolicy(policy));
    Optional.ofNullable(getTransitiveTagKeys()).ifPresent((keys) -> builder.withTransitiveTagKeys(keys));
    return builder.withSessionTags(sessionTags()).withStsClient(sts)
        .withExternalId(Password.decode(ExternalResolver.resolve(getRoleExternalId())));
  }

  private Collection<Tag> sessionTags() {
    KeyValuePairSet tags = ObjectUtils.defaultIfNull(getSessionTags(), new KeyValuePairSet());
    return tags.stream()
        .map((kvp) -> new Tag().withKey(kvp.getKey()).withValue(kvp.getValue()))
        .collect(Collectors.toList());
  }

  public STSAssumeroleCredentialsBuilder withCredentials(AWSCredentialsProviderBuilder a) {
    setCredentials(a);
    return this;
  }

  public STSAssumeroleCredentialsBuilder withRoleArn(String s) {
    setRoleArn(s);
    return this;
  }

  public STSAssumeroleCredentialsBuilder withRoleSessionName(String s) {
    setRoleSessionName(s);
    return this;
  }

  public STSAssumeroleCredentialsBuilder withRoleExternalId(String s) {
    setRoleExternalId(s);
    return this;
  }
}
