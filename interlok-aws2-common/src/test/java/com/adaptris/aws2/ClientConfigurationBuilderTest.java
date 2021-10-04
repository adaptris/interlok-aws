package com.adaptris.aws2;

import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import org.junit.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClientConfigurationBuilderTest extends ClientConfigurationBuilder {

  @Test
  public void testBuild() throws Exception {
    KeyValuePairSet cfg = new KeyValuePairSet();
    cfg.add(new KeyValuePair("CacheResponseMetadata", "true"));
    cfg.add(new KeyValuePair("ResponseMetadataCacheSize", "8192"));
    cfg.add(new KeyValuePair("zzzzzUnmatched", "true"));
    ClientOverrideConfiguration cc = build(cfg);
    assertTrue(Boolean.valueOf(cc.headers().get("CacheResponseMetadata").get(0)));
    assertEquals(8192, cc.headers().get("ResponseMetadataCacheSize").get(0));
  }
}
