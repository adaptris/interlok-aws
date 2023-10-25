package com.adaptris.aws2;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

/** Wraps {@code AwsClientBuilder.EndpointConfiguration} for configuration purposes.
 *  @config aws2-custom-endpoint
 * @since 4.3.0
 */
@XStreamAlias("aws2-custom-endpoint")
@NoArgsConstructor
public class CustomEndpoint implements EndpointBuilder {

  private transient Logger log = LoggerFactory.getLogger(this.getClass());
  /**
   * Set the custom service endpoint (e.g. {@code https://sns.us-west-1.amazonaws.com}).
   */
  @Getter
  @Setter
  private String serviceEndpoint;
  /**
   * Set the signing region for the endpoint (e.g. {@code us-west-1}).
   *
   */
  @Getter
  @Setter
  private String signingRegion;

  /**
   * Whether or not this endpoint has configuration.
   *
   * @return true if both serviceEndpoint and signing region have are non-blank
   */
  public boolean isConfigured() {
    return StringUtils.isNoneBlank(getServiceEndpoint(), getSigningRegion());
  }

  @Override
  public <T extends AwsClientBuilder<?, ?>> T rebuild(T builder) {
    log.trace("Setting EndpointConfiguration to {}:{}", getServiceEndpoint(), getSigningRegion());
    builder.endpointOverride(URI.create(getServiceEndpoint()));
    builder.region(Region.of(getSigningRegion()));
    return builder;
  }


  public CustomEndpoint withServiceEndpoint(String s) {
    setServiceEndpoint(s);
    return this;
  }


  public CustomEndpoint withSigningRegion(String s) {
    setSigningRegion(s);
    return this;
  }

}
