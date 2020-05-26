package com.adaptris.aws.apache.interceptor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpRequestInterceptor;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.aws.AWSCredentialsProviderBuilder;
import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.aws.StaticCredentialsBuilder;
import com.adaptris.core.http.apache.request.RequestInterceptorBuilder;
import com.adaptris.interlok.util.Args;
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
 * sent to AWS Elasticsearch Service
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
    summary = "Supplies an Apache HTTP Request Interceptor used to sign requests made an AWS managed Elasticsearch instance",
    tag = "amazon,aws,elastic,elasticsearch", since = "3.10.2")
@DisplayOrder(order = {"serviceName", "region", "credentials"})
@NoArgsConstructor
public class ApacheSigningInterceptor implements RequestInterceptorBuilder {

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


  @Override
  @SneakyThrows(Exception.class)
  public HttpRequestInterceptor build() {
    String service = Args.notBlank(getServiceName(), "service-name");
    String region = Args.notBlank(getRegionName(), "region-name");
    AWS4Signer signer = new AWS4Signer();
    signer.setServiceName(service);
    signer.setRegionName(region);
    return new AWSRequestSigningApacheInterceptor(service, signer, credentialsProvider().build());
  }

  private AWSCredentialsProviderBuilder credentialsProvider() {
    return ObjectUtils.defaultIfNull(getCredentials(),
        new StaticCredentialsBuilder().withAuthentication(new DefaultAWSAuthentication()));
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
