package com.adaptris.aws.sqs.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.adaptris.aws.sqs.jms.RetryPolicyBuilder;
import com.amazonaws.retry.RetryPolicy;

public class RetryPolicyBuilderTest {

  @Test
  public void testBuild() throws Exception {
    RetryPolicyBuilder b = new RetryPolicyBuilder();
    b.setBackoffStrategyClass(MyBackoffStrategy.class.getCanonicalName());
    b.setRetryConditionClass(MyRetryCondition.class.getCanonicalName());
    b.setMaxErrorRetry(10);
    b.setUseClientConfigurationMaxErrorRetry(true);
    RetryPolicy rp = b.build();
    assertEquals(MyBackoffStrategy.class, rp.getBackoffStrategy().getClass());
    assertEquals(MyRetryCondition.class, rp.getRetryCondition().getClass());
    assertEquals(10, rp.getMaxErrorRetry());
    assertTrue(rp.isMaxErrorRetryInClientConfigHonored());
  }

  @Test
  public void testBuildNullImpls() throws Exception {
    RetryPolicyBuilder b = new RetryPolicyBuilder();
    assertNotNull(b.build());
  }

  @Test
  public void testBuildBadImpls() throws Exception {
    RetryPolicyBuilder b = new RetryPolicyBuilder();
    try {
      b.setBackoffStrategyClass("blahblah");
      b.build();
      fail();
    } catch (Exception expected) {

    }
  }
}
