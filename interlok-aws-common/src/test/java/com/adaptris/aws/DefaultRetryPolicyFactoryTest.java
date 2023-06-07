package com.adaptris.aws;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import com.adaptris.aws.DefaultRetryPolicyFactory.PredefinedPolicy;

public class DefaultRetryPolicyFactoryTest {

  @Test
  public void testBuild() throws Exception {
    for (PredefinedPolicy p : PredefinedPolicy.values()) {
      assertNotNull(new DefaultRetryPolicyFactory(p).build());
    }
  }
}
