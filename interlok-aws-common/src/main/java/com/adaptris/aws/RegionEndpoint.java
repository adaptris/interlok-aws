package com.adaptris.aws;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.client.builder.AwsClientBuilder;
import lombok.AccessLevel;
import lombok.Getter;

public class RegionEndpoint implements EndpointBuilder {

  private transient Logger log = LoggerFactory.getLogger(this.getClass());

  @Getter(AccessLevel.PRIVATE)
  private transient String region = null;

  public RegionEndpoint(String r) {
    region = r;
  }

  @Override
  public <T extends AwsClientBuilder<?, ?>> T rebuild(T builder) {
    if (StringUtils.isNotBlank(getRegion())) {
      log.trace("Setting Region to {}", getRegion());
      builder.setRegion(getRegion());
    }
    return builder;
  }

}
