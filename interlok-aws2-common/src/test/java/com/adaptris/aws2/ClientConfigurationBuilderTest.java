package com.adaptris.aws2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

public class ClientConfigurationBuilderTest extends ClientConfigurationBuilder {

  @Test
  public void testBuild() throws Exception {
    KeyValuePairSet cfg = new KeyValuePairSet();
    cfg.add(new KeyValuePair("CacheResponseMetadata", "true"));
    cfg.add(new KeyValuePair("ResponseMetadataCacheSize", "8192"));
    cfg.add(new KeyValuePair("zzzzzUnmatched", "true"));
    ClientOverrideConfiguration cc = build(cfg);
    assertTrue(Boolean.valueOf(cc.headers().get("CacheResponseMetadata").get(0)));
    assertEquals(8192, Integer.parseInt(cc.headers().get("ResponseMetadataCacheSize").get(0)));
  }
}
