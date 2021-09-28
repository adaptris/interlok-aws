package com.adaptris.aws2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy.RetryCondition;

public class MockRetryCondition implements RetryCondition {

  @Override
  public boolean shouldRetry(AmazonWebServiceRequest originalRequest,
      AmazonClientException exception, int retriesAttempted) {
    return false;
  }


}
