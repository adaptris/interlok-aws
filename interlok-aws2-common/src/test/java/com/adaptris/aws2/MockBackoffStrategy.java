package com.adaptris.aws2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy.BackoffStrategy;

public class MockBackoffStrategy implements BackoffStrategy {

  @Override
  public long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest,
      AmazonClientException exception, int retriesAttempted) {
    return 0;
  }

}
