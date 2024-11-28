package com.adaptris.aws;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;


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

  @Test
  public void testBuild_ClassNotFound() throws Exception {
    PluggableRetryPolicyFactory retry = new PluggableRetryPolicyFactory().withMaxErrorRetry(99)
        .withUseClientConfigurationMaxErrorRetry(true).withBackoffStrategyClass("com.Blah");
    assertThrows(RuntimeException.class, ()->{
      retry.build();
    }, "Failed to build, class not found");
  }

}
