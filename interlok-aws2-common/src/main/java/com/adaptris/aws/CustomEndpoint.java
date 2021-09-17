package com.adaptris.aws;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/** Wraps {@code AwsClientBuilder.EndpointConfiguration} for configuration purposes.
 *  @config aws-custom-endpoint
 */
@XStreamAlias("aws-custom-endpoint")
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
  
  public CustomEndpoint() {
    
  }
  
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
    builder.setEndpointConfiguration(new EndpointConfiguration(getServiceEndpoint(), getSigningRegion()));
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
