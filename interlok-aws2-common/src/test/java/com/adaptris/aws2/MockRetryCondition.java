package com.adaptris.aws2;

import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;

public class MockRetryCondition implements RetryCondition
{

  @Override
  public boolean shouldRetry(RetryPolicyContext context) {
    return false;
  }


}
