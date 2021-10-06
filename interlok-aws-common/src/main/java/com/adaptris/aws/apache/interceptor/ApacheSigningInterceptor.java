package com.adaptris.aws.apache.interceptor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequestInterceptor;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws.AWSConnection;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.DefaultRetryPolicyFactory;
import com.adaptris.aws.EndpointBuilder;
import com.adaptris.aws.RegionEndpoint;
import com.adaptris.aws.RetryPolicyFactory;
import com.adaptris.core.http.apache.request.RequestInterceptorBuilder;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;

/**
 * {@code RequestInterceptorBuilder} implementation that creates an interceptor to sign requests
 * made to AWS using AWS4Signer
 *
 * <p>
 * Note that this uses the interceptor from
 * <a href="https://github.com/awslabs/aws-request-signing-apache-interceptor/">this github
 * project</a> verbatim. It is in fact copied locally into a {@code com.amazonaws.http} package
 * since it is not officially published in any publicly available artifact.
 * </p>
 *
 * @config aws-apache-signing-interceptor
 */
@XStreamAlias("aws-apache-signing-interceptor")
@AdapterComponent
@ComponentProfile(
    summary = "Supplies an Apache HTTP Request Interceptor used to sign requests made to AWS using AWS4Signer",
    tag = "amazon,aws,elastic,elasticsearch", since = "3.10.2")
@DisplayOrder(order = {"serviceName", "regionName", "credentials"})
@NoArgsConstructor
public class ApacheSigningInterceptor
    implements RequestInterceptorBuilder, AWSCredentialsProviderBuilder.BuilderConfig {

  /**
   * The serviceName to use with {@code AWS4Signer}.
   * <p>
   * From the {@code AWS4Signer} javadocs; this is the service name that is used when calculating
   * the signature.
   * </p>
   */
  @Getter
  @Setter
  @NotBlank
  @NonNull
  private String serviceName;
  /**
   * The region to use with {@code AWS4Signer}.
   * <p>
   * From the {@code AWS4Signer} javadocs; this is the region name that is used when calculating the
   * signature.
   * </p>
   */
  @Getter
  @Setter
  @NotBlank
  @NonNull
  private String regionName;

  /**
   * How to provide Credentials for AWS.
   * <p>
   * If not specified, then a static credentials provider with a default provider chain will be
   * used.
   * </p>
   */
  @Getter
  @Setter
  @Valid
  @InputFieldDefault(value = "aws-static-credentials-builder with default credentials")
  private AWSCredentialsProviderBuilder credentials;

  /**
   * Any specific client configuration.
   */
  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private KeyValuePairSet clientConfiguration;

  /**
   * The Retry policy if required.
   *
   */
  @Valid
  @AdvancedConfig
  @Getter
  @Setter
  private RetryPolicyFactory retryPolicy;

  /**
   * The specific region for the underlying STS credentials if required.
   * <p>
   * If not explicitly set, then the {@link #getRegionName()} is used instead. Unlike
   * {@link AWSConnection} we don't expose CustomEndpoint configuration here..
   * </p>
   */
  @Getter
  @Setter
  @AdvancedConfig(rare = true)
  private String stsRegion;


  @Override
  @SneakyThrows(Exception.class)
  public HttpRequestInterceptor build() {
    String service = Args.notBlank(getServiceName(), "service-name");
    String region = Args.notBlank(getRegionName(), "region-name");
    AWS4Signer signer = new AWS4Signer();
    signer.setServiceName(service);
    signer.setRegionName(region);
    return new AWSRequestSigningApacheInterceptor(service, signer, credentials());
  }

  protected AWSCredentialsProvider credentials() throws Exception {
    return AWSCredentialsProviderBuilder.defaultIfNull(getCredentials()).build(this);
  }

  @Override
  public KeyValuePairSet clientConfiguration() {
    return ObjectUtils.defaultIfNull(getClientConfiguration(), new KeyValuePairSet());
  }

  @Override
  public EndpointBuilder endpointBuilder() {
    return new RegionEndpoint(StringUtils.defaultIfEmpty(getStsRegion(), getRegionName()));
  }

  @Override
  public RetryPolicyFactory retryPolicy() {
    return ObjectUtils.defaultIfNull(getRetryPolicy(), new DefaultRetryPolicyFactory());
  }

  public ApacheSigningInterceptor withRegion(String region) {
    setRegionName(region);
    return this;
  }

  public ApacheSigningInterceptor withService(String service) {
    setServiceName(service);
    return this;
  }

  public ApacheSigningInterceptor withCredentials(AWSCredentialsProviderBuilder creds) {
    setCredentials(creds);
    return this;
  }
}
