package com.adaptris.aws2;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class PluggableRetryPolicyFactoryTest {

  @Test
  public void testBuild() throws Exception {
    String backOff = MockBackoffStrategy.class.getCanonicalName();
    String conditionClass = MockRetryCondition.class.getCanonicalName();
    PluggableRetryPolicyFactory retry = new PluggableRetryPolicyFactory().withMaxErrorRetry(99)
        .withUseClientConfigurationMaxErrorRetry(true).withBackoffStrategyClass(backOff)
        .withRetryConditionClass(conditionClass);
    assertNotNull(retry.build());
  }

  @Test
  public void testBuild_NullClasses() throws Exception {
    PluggableRetryPolicyFactory retry = new PluggableRetryPolicyFactory().withMaxErrorRetry(99)
        .withUseClientConfigurationMaxErrorRetry(true);
    assertNotNull(retry.build());
  }

  @Test(expected = RuntimeException.class)
  public void testBuild_ClassNotFound() throws Exception {
    PluggableRetryPolicyFactory retry = new PluggableRetryPolicyFactory().withMaxErrorRetry(99)
        .withUseClientConfigurationMaxErrorRetry(true).withBackoffStrategyClass("com.Blah");
    retry.build();
  }

}
