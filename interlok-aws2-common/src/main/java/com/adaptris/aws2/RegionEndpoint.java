package com.adaptris.aws2;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

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
      builder.region(Region.of(getRegion()));
    }
    return builder;
  }

}
