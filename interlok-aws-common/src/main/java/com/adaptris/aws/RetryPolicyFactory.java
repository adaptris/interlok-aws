package com.adaptris.aws;

import com.amazonaws.retry.RetryPolicy;

public interface RetryPolicyFactory {

  /**
   * Build a retry policy.
   * 
   * @return the retry policy.
   */
  RetryPolicy build();
}
