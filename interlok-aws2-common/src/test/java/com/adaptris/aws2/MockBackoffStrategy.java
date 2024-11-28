package com.adaptris.aws2;

import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;

import java.time.Duration;

public class MockBackoffStrategy implements BackoffStrategy
{
  @Override
  public Duration computeDelayBeforeNextRetry(RetryPolicyContext context)
  {
    return Duration.ZERO;
  }
}
