package com.adaptris.aws2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.amazonaws.ClientConfiguration;

public class ClientConfigurationBuilderTest extends ClientConfigurationBuilder {

  @Test
  public void testBuild() throws Exception {
    KeyValuePairSet cfg = new KeyValuePairSet();
    cfg.add(new KeyValuePair("CacheResponseMetadata", "true"));
    cfg.add(new KeyValuePair("ResponseMetadataCacheSize", "8192"));
    cfg.add(new KeyValuePair("zzzzzUnmatched", "true"));
    ClientConfiguration cc = build(cfg);
    assertTrue(cc.getCacheResponseMetadata());
    assertEquals(8192, cc.getResponseMetadataCacheSize());
  }
}
