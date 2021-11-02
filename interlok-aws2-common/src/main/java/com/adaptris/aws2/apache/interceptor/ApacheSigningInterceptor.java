package com.adaptris.aws2.apache.interceptor;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws2.AWSConnection;
import com.adaptris.aws2.AWSCredentialsProviderBuilder;
import com.adaptris.aws2.DefaultRetryPolicyFactory;
import com.adaptris.aws2.EndpointBuilder;
import com.adaptris.aws2.RegionEndpoint;
import com.adaptris.aws2.RetryPolicyFactory;
import com.adaptris.core.http.apache.request.RequestInterceptorBuilder;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequestInterceptor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.regions.Region;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

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
 * @config aws2-apache-signing-interceptor
 * @since 4.3.0
 */
@XStreamAlias("aws2-apache-signing-interceptor")
@AdapterComponent
@ComponentProfile(
    summary = "Supplies an Apache HTTP Request Interceptor used to sign requests made to AWS using AWS4Signer",
    tag = "amazon,aws2,elastic,elasticsearch", since = "4.3.0")
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
  @InputFieldDefault(value = "aws2-static-credentials-builder with default credentials")
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
    Aws4Signer signer = Aws4Signer.create();

    Aws4PresignerParams.Builder presignParams = Aws4PresignerParams.builder();
    presignParams.awsCredentials(credentials().resolveCredentials());
    presignParams.signingName(service);
    presignParams.signingRegion(Region.of(region));

    return new AWSRequestSigningApacheInterceptor(service, signer, credentials(), presignParams.build());
  }

  protected AwsCredentialsProvider credentials() throws Exception {
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
