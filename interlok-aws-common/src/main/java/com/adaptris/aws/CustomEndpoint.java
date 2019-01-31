package com.adaptris.aws;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/** Wraps {@code AwsClientBuilder.EndpointConfiguration} for configuration purposes.
 *  @config aws-custom-endpoint
 */
@XStreamAlias("aws-custom-endpoint")
public class CustomEndpoint implements EndpointBuilder {

  private String serviceEndpoint;
  private String signingRegion;
  
  public CustomEndpoint() {
    
  }
  
  public <T extends AwsClientBuilder<?, ?>> T rebuild(T builder) {
     builder.setEndpointConfiguration(new EndpointConfiguration(getServiceEndpoint(), getSigningRegion()));
     return builder;
  }
  
  public String getServiceEndpoint() {
    return serviceEndpoint;
  }

  /** Set the service endpoint.
   * 
   * @param s the service endpoint e.g. {@code https://sns.us-west-1.amazonaws.com}.
   */
  public void setServiceEndpoint(String s) {
    this.serviceEndpoint = s;
  }

  public CustomEndpoint withServiceEndpoint(String s) {
    setServiceEndpoint(s);
    return this;
  }
  
  public String getSigningRegion() {
    return signingRegion;
  }

  /** Set the signing region.
   * 
   * @param s the region to use for SigV4 signing of requests (e.g. us-west-1)
   */
  public void setSigningRegion(String s) {
    this.signingRegion = s;
  }
  
  public CustomEndpoint withSigningRegion(String s) {
    setSigningRegion(s);
    return this;
  }
  
}
