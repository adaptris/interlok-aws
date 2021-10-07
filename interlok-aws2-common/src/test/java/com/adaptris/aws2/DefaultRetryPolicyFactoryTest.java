package com.adaptris.aws2;

import com.adaptris.aws2.DefaultRetryPolicyFactory.PredefinedPolicy;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DefaultRetryPolicyFactoryTest {

  @Test
  public void testBuild() throws Exception {
    for (PredefinedPolicy p : PredefinedPolicy.values()) {
      assertNotNull(new DefaultRetryPolicyFactory(p).build());
    }
  }
}
